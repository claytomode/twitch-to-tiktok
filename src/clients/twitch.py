"""Twitch client module."""

import yt_dlp
import httpx

import asyncio
from typing import Any
from functools import partial

class TwitchClient:
    """
    Asynchronous client for interacting with the Twitch API.

    This client uses a shared httpx.AsyncClient and is best used
    as an async context manager to handle setup and teardown.

    VOD downloading requires 'yt-dlp' and 'ffmpeg' to be installed.

    Usage:
    async with TwitchClient(id, secret) as client:
        vods = await client.list_recent_vods("streamer_name")
        # await client.download_vod(vods[0]['id'], "my_video.mp4", end_time="0:0:30")
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        api_base: str = "https://api.twitch.tv/helix",
        auth_url: str = "https://id.twitch.tv/oauth2/token",
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_base = api_base
        self.auth_url = auth_url

        self._client: httpx.AsyncClient | None = None
        self._auth_token: str | None = None
        
        if yt_dlp is None:
            print(
                "Warning: 'yt-dlp' not found. "
                "Call to download_vod or download_vod_audio will fail."
            )

    async def __aenter__(self):
        """Initialize the shared httpx client and authenticate."""
        self._client = httpx.AsyncClient(timeout=30.0)
        self._auth_token = await self._authenticate()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close the shared httpx client."""
        if self._client:
            await self._client.aclose()
        self._client = None
        self._auth_token = None

    def _assert_client(self) -> httpx.AsyncClient:
        """Internal helper to ensure the client is initialized."""
        if not self._client:
            raise RuntimeError(
                "TwitchClient is not initialized. "
                "Please use it as an async context manager: "
                "'async with TwitchClient(...) as client:'"
            )
        return self._client
        
    def _get_headers(self) -> dict[str, str]:
        """Internal helper to get auth headers."""
        if not self._auth_token:
                 raise RuntimeError(
                     "Auth token is not set. Client may not be initialized correctly."
                 )
        return {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self._auth_token}",
        }

    async def _authenticate(self) -> str:
        """Obtain an OAuth token from Twitch using the shared client."""
        client = self._assert_client()
        
        response = await client.post(
            self.auth_url,
            params={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
        )
        response.raise_for_status()
        data = response.json()
        return data["access_token"]

    async def _get_broadcaster_id_from_name(self, broadcaster_name: str) -> str:
        """Fetch broadcaster ID from a login name."""
        client = self._assert_client()
        headers = self._get_headers()
        
        response = await client.get(
            f"{self.api_base}/users",
            headers=headers,
            params={"login": broadcaster_name},
        )
        response.raise_for_status()
        data = response.json().get("data")
        if not data:
            raise ValueError(f"No user found with name: {broadcaster_name}")
        return data[0]["id"]

    async def list_recent_vods(
        self, 
        broadcaster_name: str, 
        limit: int = 20
    ) -> list[dict[str, Any]]:
        """
        Lists the most recent VODs (type: 'archive') for a given broadcaster.
        """
        client = self._assert_client()
        headers = self._get_headers()
        broadcaster_id = await self._get_broadcaster_id_from_name(broadcaster_name)
        
        params = {
            "user_id": broadcaster_id,
            "type": "archive",
            "first": max(1, min(limit, 100))
        }
        
        response = await client.get(
            f"{self.api_base}/videos", 
            headers=headers, 
            params=params
        )
        response.raise_for_status()
        return response.json().get("data", [])

    async def get_vod_thumbnail_url(
        self, 
        video_id: str, 
        width: int = 1920, 
        height: int = 1080
    ) -> str:
        """
        Gets a specific VOD's thumbnail URL, formatted to the desired size.
        """
        client = self._assert_client()
        headers = self._get_headers()
        
        params = {"id": video_id}
        response = await client.get(
            f"{self.api_base}/videos", 
            headers=headers, 
            params=params
        )
        response.raise_for_status()
        
        data = response.json().get("data")
        if not data:
            raise ValueError(f"No VOD found with ID: {video_id}")
            
        thumbnail_url: str = data[0]["thumbnail_url"]
        
        return thumbnail_url.replace("{width}", str(width)).replace(
            "{height}", str(height)
        )

    async def _download_with_yt_dlp(
        self, 
        url: str, 
        ydl_opts: dict[str, Any]
    ):
        """
        Internal async helper to run synchronous yt-dlp download in a thread.
        """
        if yt_dlp is None:
            raise ImportError(
                "yt-dlp library is not installed. "
                "Please run: pip install yt-dlp"
            )
        
        class YtdlpLogger:
            def debug(self, msg):
                # Print all debug messages (like extractor info)
                print(f"[YTDLP_DEBUG] {msg}")
            def info(self, msg):
                # Print info messages (like download progress)
                print(f"[YTDLP_INFO] {msg}")
            def warning(self, msg):
                print(f"[YTDLP_WARN] {msg}")
            def error(self, msg):
                print(f"[YTDLP_ERROR] {msg}")

        ydl_opts.setdefault('logger', YtdlpLogger())
        ydl_opts['verbose'] = True

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                download_func = partial(ydl.download, [url])
                await asyncio.to_thread(download_func)
            
        except Exception as e:
            print(f"An error occurred during download: {e}")
            raise

    async def _download_media(
        self,
        video_id: str,
        output_path: str,
        start_time: str | None,
        end_time: str | None,
        base_ydl_opts: dict[str, Any]
    ):
        """Internal helper to handle all download logic."""
        
        url = f"https://www.twitch.tv/videos/{video_id}"
        
        ydl_opts = base_ydl_opts.copy()
        ydl_opts['outtmpl'] = output_path
        
        
        if (start_time or end_time):
            input_args = []
            if start_time:
                input_args.extend(['-ss', start_time])
            if end_time:
                input_args.extend(['-to', end_time])

            output_args = ['-loglevel', 'info']

            ydl_opts['external_downloader'] = 'ffmpeg'
            ydl_opts['external_downloader_args'] = {
                'ffmpeg_i': input_args,
                'default': output_args,
            }
            
            ydl_opts.pop('download_sections', None)
            
        await self._download_with_yt_dlp(url, ydl_opts)

    async def download_vod(
        self,
        video_id: str,
        output_path: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ):
        """
        Downloads a VOD (or a segment) using yt-dlp.

        Requires 'yt-dlp' and 'ffmpeg' to be installed on the system.
        """
        
        ydl_opts: dict[str, Any] = {
            'format': 'bestvideo[protocol=m3u8_native]+bestaudio[protocol=m3u8_native]/best[protocol=m3u8_native]',
            'overwrites': True,
        }
        
        await self._download_media(
            video_id=video_id,
            output_path=output_path,
            start_time=start_time,
            end_time=end_time,
            media_type_log="VOD",
            base_ydl_opts=ydl_opts
        )

    async def download_vod_audio(
        self,
        video_id: str,
        output_path: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ):
        """
        Downloads only the audio from a VOD (or a segment) using yt-dlp.

        Requires 'yt-dlp' and 'ffmpeg' to be installed on the system.
        """
        ydl_opts: dict[str, Any] = {
            'format': 'bestaudio[protocol=m3u8_native]/bestaudio',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'm4a', 
            }],
            'overwrites': True,
        }
        
        await self._download_media(
            video_id=video_id,
            output_path=output_path,
            start_time=start_time,
            end_time=end_time,
            media_type_log="VOD audio",
            base_ydl_opts=ydl_opts
        )