"""m3u8(세그먼트 분할) 다운로드 엔진 — BaseDownloader 하위 구현 (#74, #82, SPEC §5·§6).

공통 실행 엔진(워커 풀·스케일링·관측·재큐잉·일시정지)은
core/downloaders/base.py의 BaseDownloader로 이주했다(#82). 이 클래스에는
m3u8 고유 부분만 남는다:

- prepare: 플레이리스트를 받아 세그먼트 목록의 DownloadPlan 생성 (#83 —
  총 크기 미상이라 total_size=None, 병합이 필요해 requires_postprocess=True).
  base_url은 m3u8
  플레이리스트 URL이며 **호출 전에 해석이 끝나 있어야 한다** —
  requires_base_url_resolution=True로 서비스가 주입된 resolver를 시작
  시점에 실행한다 (쿠키·치지직 API 조회는 아직 앱 영역)
- _prepare_output: 저장된 조각 검증 + EXT-X-MAP 초기화 세그먼트 갱신
- _download_segment: 세그먼트 단위 다운로드 — 저속 재시도·일시정지·중단
  핸들링 포함. 규칙은 tests/unit/core/test_m3u8_downloader_rules.py가 박제한다
- postprocess: 세그먼트를 인덱스 순서로 ffmpeg stdin에 흘리는 단일 패스
  재포장 (#88·#92) — 시작은 on_merge_start 콜백으로 알리고, 실패 시 폴백
  없이 명확히 실패한다 (세그먼트 보존)
- 전체 크기를 미리 알 수 없어 ProgressEvent.total_size는 None이다.
  진행률 계산(세그먼트 수 기반)은 어댑터의 몫이다.
"""

import os
import logging
import time as tm
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import requests

import core.downloaders.base as base_module
import core.downloaders.integrity as integrity
from core.api.session import get_thread_session
from core.downloaders.base import REQUEST_TIMEOUT, BaseDownloader
from core.models.content import Content, ContentType
from core.models.download_state import DownloadState
from core.models.plan import DownloadPlan
from core.utils.paths import choose_temp_dir  # noqa: F401 — 기존 테스트의 주입 지점
from core.utils.persistence import publish_file
from core.downloaders.resume import ResumeStore, ResumeMismatchError, digest_file


# 초기화 세그먼트를 다시 받게 하는 예외 — 본문이 잘려 왔다는 뜻인 것만 (#321).
# ChunkedEncodingError는 본문이 선언된 길이보다 먼저 끝났을 때, ContentDecodingError는
# 압축된 본문이 중간에 끊겨 풀리지 않을 때 난다
_INIT_TRUNCATION_ERRORS = (
    integrity.TruncatedSegmentError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ContentDecodingError,
)


def _cache_recovery_url(url: str) -> str:
    """실측된 네이버 CDN에서 손상 캐시를 피하도록 파일 앞 구분자를 중복한다.

    2026-10-12 실측: 일반 요청·Range·no-cache·쿼리 변경 모두 같은 손상 본문을
    반환했지만 이 경로는 정상 본문을 반환했다. 호스트·파일·인증 정보는 유지한다.
    다른 서버의 경로 의미는 알 수 없으므로 이 CDN에만 적용한다. 수신 후 구조 검사는
    그대로 수행하며 반복 실패해도 구분자를 계속 늘리지 않는다.
    """
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != "ex-nlive-slitvod-streaming.navercdn.com":
        return url
    directory, separator, filename = parsed.path.rpartition("/")
    if not separator or not filename or directory.endswith("/"):
        return url
    return parsed._replace(path=directory + "//" + filename).geturl()


class M3U8Downloader(BaseDownloader):
    """m3u8 VOD를 세그먼트 단위 멀티스레드로 다운로드·병합하는 엔진.

    호출 규약: 소유자(서비스·스크립트)가 data.base_url에 m3u8 플레이리스트 URL을
    채우고 DownloadTaskModel.start()로 RUNNING 전이를 마친 뒤 run()을 호출한다.
    run()은 완료·중단·실패까지 블로킹한다.
    """

    run_thread_name = "DownloadM3U8Thread"
    worker_pool_prefix = "DownloadM3U8Worker"
    requires_base_url_resolution = True
    # 구 코드와 동일하게 모든 예외를 실패로 처리한다 (플레이리스트 파싱 실패 포함)
    _failure_exceptions = (Exception,)

    def __init__(self, data, logger, **callbacks):
        super().__init__(data, logger, **callbacks)
        # 운영체제 임시 정리로 유일한 복구본이 사라지지 않도록 저장 폴더에 둔다.
        self._store = ResumeStore(self.s.content)
        self.temp_dir = str(self._store.directory)
        self._verified = {}
        self._cache_recovery: set[int] = set()
        self._completed = False
        self._failure = None
        self._notify_failed = self._on_failed
        self._notify_finished = self._on_finished
        self._on_failed = self._remember_failure
        self._on_finished = self._remember_finished

    def _remember_failure(self, exc: BaseException) -> None:
        if self._failure is None:
            self._failure = exc

    def _remember_finished(self) -> None:
        self._completed = True

    def run(self) -> None:
        """모든 쓰기와 복구 기록 저장을 마친 후에만 실패 또는 완료를 알린다."""
        try:
            self._store.open()
            if (
                self._store.record.get("complete")
                and self._store.record.get("output") != Path(self.s.output_path).name
            ):
                self._store.record = {}
            if self._store.record.get("finalizing"):
                output = Path(self.s.output_path)
                if output.is_file() and digest_file(output) == self._store.record.get(
                    "output_sha256"
                ):
                    self._store.record.update(complete=True, finalizing=False)
                    self._store.flush()
            if self._store.record.get("complete"):
                output = Path(self.s.output_path)
                if output.is_file() and digest_file(output) == self._store.record.get(
                    "output_sha256"
                ):
                    self._completed = True
                    self.s.start_time = self.s.end_time = tm.time()
                    self._cleanup_after_run()
                    self.logger.save_and_close()
                else:
                    raise ResumeMismatchError("완료 파일을 확인할 수 없습니다. 데이터는 보존됩니다")
            else:
                super().run()
        except Exception as exc:
            self._remember_failure(exc)
            self.logger.log_exception("Resume failed; saved data retained", exc)
            self.logger.save_and_close()
        finally:
            try:
                self._store.close()
            except OSError as exc:
                if not self._completed:
                    self._remember_failure(exc)
                self.logger.log_error("Resume record could not be saved", exc)
        if self._failure is not None:
            self._notify_failed(self._failure)
        elif self._completed:
            self._notify_finished()

    @classmethod
    def supports(cls, content: Content) -> bool:
        """라이브 다시보기(m3u8) VOD를 처리한다."""
        return content.content_type is ContentType.CHZZK_VIDEO_M3U8

    # ============ 작업 목록·수신 준비 (구 run의 m3u8 고유 부분) ============

    def prepare(self, content: Content) -> DownloadPlan:
        """플레이리스트를 받아 (index, 세그먼트) 목록의 계획을 만든다."""
        response = get_thread_session().get(self.s.base_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        lines = response.text.splitlines()
        segments = [line for line in lines if line and not line.startswith("#")]
        # EXT-X-MAP(초기화 세그먼트) 추출을 위해 _prepare_output이 참조한다
        self._playlist_lines = lines
        self._segments = segments
        if not segments:
            raise ResumeMismatchError("영상 조각 목록이 비어 있습니다")
        if any(line.startswith("#EXT-X-BYTERANGE") for line in lines):
            raise ResumeMismatchError("범위 기반 재생 목록의 이어받기는 지원하지 않습니다")
        if sum(line.startswith("#EXT-X-MAP:") for line in lines) != 1 or any(
            line.startswith("#EXT-X-MAP:") and "BYTERANGE=" in line for line in lines
        ):
            raise ResumeMismatchError("복수 초기화 조각 또는 범위 초기화 목록은 지원하지 않습니다")
        self.s.merged_segments = 0
        # 세그먼트 임시 파일명 0채움 자릿수 — sorted() 병합 순서의 전제
        self.width = len(str(len(segments)))
        # 총 바이트 크기는 미리 알 수 없고(total_size=None), 병합 후처리가 필요하다
        return DownloadPlan(
            items=tuple(enumerate(segments)),
            total_size=None,
            requires_postprocess=True,
        )

    def _download_start_log_args(self) -> tuple:
        # m3u8은 전체 크기·파트 크기를 미리 알 수 없다 (구 코드와 동일하게 0)
        return (0, 0, self.s.total_ranges, self.s.adjust_threads)

    def _prepare_output(self) -> None:
        """임시 폴더를 만들고 초기화 세그먼트(EXT-X-MAP)를 받아 온전한지 확인한다 (#321).

        초기화 세그먼트가 잘려 받아졌으면 다시 받는다. 세그먼트의 일시 오류와 같은 횟수
        (base의 _TRANSIENT_ERROR_REQUEUE_LIMIT)까지 다시 받고, 그래도 안 되면 마지막 예외를
        그대로 던져 실패시킨다 — 잘린 초기화 세그먼트로는 영상 전체를 읽을 수 없다.

        다시 받는 것은 잘림 계열의 예외뿐이다(_INIT_TRUNCATION_ERRORS). 본문이 선언된
        길이보다 짧으면 구조 검사에 닿기 전에 본문을 읽는 단계에서 요청 예외가 난다 —
        그것도 잘린 것이다. 연결 실패 · 타임아웃 같은 그 밖의 네트워크 오류는 준비 단계의
        다른 요청과 같이 다시 받지 않고 그대로 실패한다(#320).

        예외의 종류는 바꾸지 않는다. 본문을 읽다 난 요청 예외는 연결이 끊겨도 나므로
        잘린 것으로 단정할 수 없다 — 잘림 실패(TruncatedSegmentError)는 내용 검사로
        확인된 경우에만 나간다. 미디어 세그먼트의 재큐와 같은 규칙이다.

        Raises:
            TruncatedSegmentError: 다시 받아도 초기화 세그먼트의 구조가 계속 온전하지 않을 때
            requests.RequestException: 다시 받아도 본문을 끝까지 읽지 못할 때
        """
        retries = base_module._TRANSIENT_ERROR_REQUEUE_LIMIT
        for attempt in range(retries + 1):
            try:
                self._receive_init_segment()
                self._verified = self._store.verified()
                missing = set(range(len(self._segments))) - self._verified.keys()
                self._anchors = sorted(i - 1 for i in missing if i > 0 and i - 1 not in missing)
                reused = self._verified.keys() - set(self._anchors)
                self.s.resume_reused = len(reused)
                self.s.resume_missing = len(missing)
                self.s.resume_preceding = len(self._anchors)
                self.s.completed_threads = len(reused)
                self.s.completed_progress = sum(self._verified[i] for i in reused)
                self.s.total_downloaded_size = self.s.completed_progress
                self.s.prev_size = self.s.completed_progress
                logging.getLogger(__name__).info(
                    "Resume: reused=%s missing=%s preceding=%s",
                    len(reused),
                    len(missing),
                    len(self._anchors),
                )
                return
            except _INIT_TRUNCATION_ERRORS as e:
                if attempt == retries:
                    raise
                self.logger.log_error("Init segment incomplete — retrying", e)

    def _receive_init_segment(self) -> None:
        """새 초기화 조각을 검증하고 기존 목록과 호환될 때만 확정한다."""
        os.makedirs(self.temp_dir, exist_ok=True)

        init_segment = None
        for line in self._playlist_lines:
            if line.startswith("#EXT-X-MAP:"):
                init_segment = line.split('URI="', 1)[1].split('"', 1)[0]
        if init_segment is None:
            raise ResumeMismatchError("초기화 조각이 없는 재생 목록입니다")
        init_url = urljoin(self.s.base_url, init_segment)
        init_segment_path = os.path.join(self.temp_dir, f"{0:0{self.width}d}.m4s")
        response = get_thread_session().get(init_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.content
        integrity.check_fmp4_init_segment(payload)
        self._store.adopt(self._playlist_lines, self._segments, payload, self.s.output_path)
        receiving = init_segment_path + ".receiving"
        with open(receiving, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(receiving, init_segment_path)

    def _initial_queue(self, items: list) -> list:
        """누락 구간의 직전 정상 조각을 먼저 받은 뒤 빠진 조각만 큐에 넣는다."""
        for index in self._anchors:
            if self.state == DownloadState.WAITING:
                break
            self._download_segment(index, self._segments[index], 0, self.s.total_ranges)
        return [item for item in items if item[0] not in self._verified]

    def _log_item_start(self, part_num: int, item) -> None:
        _index, segment = item
        self.logger.log_m3u8_thread_start(part_num, f"segment #{_index + 1}")

    def _download_item(self, item, part_num: int):
        index, segment = item
        return self._download_segment(
            index=index, segment=segment, part_num=part_num, total_ranges=self.s.total_ranges
        )

    def _cleanup_partial(self) -> None:
        """실패와 중지에서는 원본 조각을 남기고 복구 기록만 저장한다."""
        self._store.flush()

    def _cleanup_after_run(self) -> None:
        """확정된 MP4가 있을 때만 조각을 정리하고 완료 영수증은 남긴다."""
        if not self._completed:
            self._store.flush()
            return
        try:
            for path in self._store.directory.iterdir():
                if path.name.endswith((".m4v", ".m4s", ".part", ".receiving")) and path.is_file():
                    path.unlink()
        except OSError as exc:
            self.logger.log_error("Completed video retained; cleanup deferred", exc)

    def _progress_total_size(self) -> int | None:
        # 전체 바이트 크기를 미리 알 수 없다 — 세그먼트 수 기반 계산은 어댑터가 한다
        return None

    # ============ 후처리: 순서 보장 병합 (구 run의 (4)) ============

    def postprocess(self) -> None:
        """세그먼트들을 인덱스 순서 그대로 ffmpeg stdin에 흘려 mp4로 재포장한다 (#88·#92).

        바이트 연결만으로는 fMP4 조각 구조(전역 인덱스 없음·라이브 타임라인
        보유)가 그대로라 편집 프로그램이 읽지 못한다. #92부터 중간 병합 파일
        없이 단일 패스로 재포장하며, 실패 시 폴백 없이 명확히 실패하고
        세그먼트를 보존한다 (규칙은 base의 _remux_streamed 참조).
        """
        self._store.record["phase"] = "merging"
        self._store.flush()
        self._on_merge_start()
        if len(self._store.verified()) != self._store.count:
            raise integrity.TruncatedSegmentError("병합할 정상 조각이 모두 준비되지 않았습니다")
        segment_paths = [os.path.join(self.temp_dir, f"{0:0{self.width}d}.m4s")]
        segment_paths.extend(str(self._store.segment_path(i)) for i in range(self._store.count))
        expected = {Path(path).name for path in segment_paths} | {"resume.json", ".lock"}
        stray = sorted(
            path.name
            for path in self._store.directory.iterdir()
            if path.name not in expected and not path.name.endswith((".part", ".receiving"))
        )
        if stray:
            self.logger.warning("Ignoring unrelated files: " + ", ".join(stray))
        output = self.s.output_path
        building = str(self._store.directory / "result.building")
        try:
            self.s.output_path = building
            self._remux_streamed(segment_paths)
        finally:
            self.s.output_path = output
        if self.state == DownloadState.WAITING:
            return
        if not os.path.isfile(building) or os.path.getsize(building) == 0:
            raise OSError("병합 결과가 비어 있습니다")
        if os.path.exists(output):
            raise ResumeMismatchError("같은 이름의 파일이 이미 있습니다. 기존 파일은 보존됩니다")
        # 파일 확정 이전에 영수증을 저장하면 그 직후 종료돼도 생성 중 파일로 복구한다.
        self._store.record["output_sha256"] = digest_file(Path(building))
        self._store.record["finalizing"] = True
        self._store.flush()
        publish_file(Path(building), Path(output))
        self._store.record["complete"] = True
        self._store.record["finalizing"] = False
        self._store.flush()

    # ============ 다운로드 동작 관련 메서드들 ============

    def _download_segment(self, index: int, segment: str, part_num: int, total_ranges: int):
        """
        개별 세그먼트 다운로드(재시도 포함)
        """
        downloaded_size = 0
        segment_url = urljoin(self.s.base_url, segment)
        if index in self._cache_recovery:
            segment_url = _cache_recovery_url(segment_url)
        temp_file = Path(self.temp_dir) / f"{index + 1:0{self.width}d}.m4v.receiving"
        response = None
        while not self.state == DownloadState.WAITING:
            try:
                # 스레드로컬 세션으로 같은 워커의 세그먼트 요청 간 연결을 재사용한다 (#31)
                response = get_thread_session().get(
                    segment_url, stream=True, timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()
                part_start_time = self._now()
                slow_watch = self._watch_slow(
                    part_num, integrity.declared_length(getattr(response, "headers", None))
                )
                # 디스크 쓰기 누적 시간 — 저속 판정에는 더 이상 반영하지 않는다(#191).
                # f.write()는 OS 페이지 캐시에 즉시 반환되는 버퍼드 쓰기라 실기
                # 로그(write=0.000s/0.494s=0%)로 기여도가 정확히 0%임을 확인했다 —
                # 뺄 게 없어 판정에 실질적 영향이 없었다. 계측·진단 목적으로만
                # 남긴다(#191 이슈 기록 참조) — 판정에는 관여하지 않는다
                write_elapsed = 0.0

                with open(temp_file, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if self.state == DownloadState.WAITING:
                            return part_num
                        part_start_time += self._wait_while_paused()

                        if chunk:
                            write_start = tm.perf_counter()
                            f.write(chunk)
                            write_elapsed += tm.perf_counter() - write_start
                            downloaded_size += len(chunk)
                            elapsed = self._now() - part_start_time

                            if elapsed > 0:
                                speed_kb_s = downloaded_size / elapsed / 1024
                                self._check_speed_and_update_progress(
                                    part_num, downloaded_size, total_ranges, speed_kb_s
                                )
                                if slow_watch.is_slow(elapsed, downloaded_size):
                                    if index in self._verified:
                                        self._reuse_preceding(index, part_num)
                                        return part_num
                                    # 속도가 너무 느리면 스레드 재시작
                                    ratio = write_elapsed / elapsed * 100 if elapsed > 0 else 0.0
                                    diagnostic = (
                                        f"write={write_elapsed:.3f}s/{elapsed:.3f}s={ratio:.0f}%"
                                    )
                                    with self.lock:
                                        self._requeue_slow(
                                            (index, segment), part_num, diagnostic=diagnostic
                                        )
                                    self._abandon_response(part_num, response)
                                    response = None
                                    return part_num
                    f.flush()
                    os.fsync(f.fileno())

                # 받은 세그먼트가 온전한지 내용으로 확인한다 (#321) — 잘린 본문이
                # 200과 맞는 Content-Length로 올 수 있어 상태 코드로는 알 수 없다.
                # 온전하지 않으면 아래 except가 일시 오류로 다시 받게 한다
                integrity.check_content_length(getattr(response, "headers", None), downloaded_size)
                with open(temp_file, "rb") as f:
                    integrity.check_fmp4_media_segment(f, downloaded_size)
                if self._store.record:
                    self._store.commit(index, temp_file)
                else:
                    # 단일 조각 실행을 사용하는 기존 주입 경로도 원자적으로 확정한다.
                    os.replace(temp_file, str(temp_file).removesuffix(".receiving"))

                # 성공적으로 마무리된 경우
                with self.lock:
                    self.s.completed_threads += 1
                    self.s.completed_progress += downloaded_size
                    self.s.threads_progress[part_num] = 0
                self.logger.log_thread_complete(part_num, downloaded_size)
                return part_num

            except (
                requests.RequestException,
                requests.Timeout,
                integrity.TruncatedSegmentError,
            ) as e:
                if isinstance(e, integrity.TruncatedSegmentError):
                    if (
                        index not in self._cache_recovery
                        and _cache_recovery_url(segment_url) != segment_url
                    ):
                        self._cache_recovery.add(index)
                        self.logger.warning(
                            f"Segment {index + 1}: invalid cached body; retrying alternate resource path"
                        )
                self._store.record["last_failed_segment"] = index
                if index in self._verified:
                    self._reuse_preceding(index, part_num)
                    return part_num
                with self.lock:
                    self._requeue_failed((index, segment), part_num, e)
                self.logger.log_error(f"Part {part_num} download failed (segment {index + 1})", e)
                return part_num
            finally:
                if response is not None:
                    getattr(response, "close", lambda: None)()
                if temp_file.exists():
                    os.replace(temp_file, str(temp_file).removesuffix(".receiving") + ".part")

    def _reuse_preceding(self, index: int, part_num: int) -> None:
        """직전 조각을 다시 받지 못해도 이미 검증된 정상 파일은 재사용한다."""
        with self.lock:
            self.s.completed_threads += 1
            self.s.completed_progress += self._verified[index]
            self.s.prev_size += self._verified[index]
            self.s.threads_progress[part_num] = 0
