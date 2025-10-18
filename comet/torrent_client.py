import libtorrent as lt
import asyncio
import time
import os
import re

from comet.utils.logger import logger
from comet.utils.models import settings

class TorrentClient:
    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TorrentClient, cls).__new__(cls)
        return cls._instance

    async def init_session(self):
        async with self._lock:
            if hasattr(self, 'session'):
                return

            self.session = lt.session({
                'listen_interfaces': '0.0.0.0:6881',
                'download_rate_limit': 0,
                'upload_rate_limit': 0,
            })
            self.handles = {}
            self.download_path = settings.TORRENT_DOWNLOAD_PATH
            if not os.path.exists(self.download_path):
                os.makedirs(self.download_path, exist_ok=True)

            logger.info("Torrent client session started.")
            asyncio.create_task(self._session_stats())

    async def _session_stats(self):
        while True:
            s = self.session.status()
            logger.info(
                f"{s.dht_nodes} DHT nodes, {s.num_peers} peers, "
                f"{(s.download_rate / 1000):.1f} kB/s down, "
                f"{(s.upload_rate / 1000):.1f} kB/s up"
            )
            await asyncio.sleep(5)

    async def add_torrent(self, magnet_link):
        if not hasattr(self, 'session'):
            await self.init_session()

        info_hash = self._get_info_hash_from_magnet(magnet_link)
        if not info_hash:
            logger.error(f"Could not extract info_hash from magnet: {magnet_link}")
            return None

        if info_hash in self.handles:
            logger.info(f"Torrent {info_hash} is already in session.")
            return self.handles[info_hash]

        params = lt.parse_magnet_uri(magnet_link)
        params.save_path = self.download_path
        params.storage_mode = lt.storage_mode_t.storage_mode_sparse

        handle = self.session.add_torrent(params)
        self.handles[info_hash] = handle
        logger.info(f"Added torrent {info_hash} for download.")

        asyncio.create_task(self._wait_for_metadata(handle))
        return handle

    async def _wait_for_metadata(self, handle):
        while not handle.has_metadata():
            await asyncio.sleep(1)

        info = handle.get_torrent_info()
        logger.info(f"Metadata received for torrent {info.info_hash()}. Name: {info.name()}")

    def get_handle(self, info_hash):
        return self.handles.get(info_hash.lower())

    def get_file_path(self, handle, file_index):
        if not handle or not handle.is_valid() or not handle.has_metadata():
            return None

        ti = handle.get_torrent_info()
        fs = ti.files()
        if file_index >= fs.num_files():
            return None

        file_entry = fs.file_at(file_index)
        torrent_name = ti.name()
        full_path = os.path.join(self.download_path, torrent_name, file_entry.path)
        return full_path

    async def wait_for_file_ready(self, info_hash, file_index, timeout=120):
        handle = self.get_handle(info_hash)
        if not handle:
            logger.warning(f"No handle found for info_hash {info_hash} during wait.")
            return None

        start_time = time.time()
        while time.time() - start_time < timeout:
            if not handle.has_metadata():
                logger.debug(f"Waiting for metadata for {info_hash}...")
                await asyncio.sleep(2)
                continue

            file_path = self.get_file_path(handle, file_index)
            if file_path and os.path.exists(file_path) and os.path.getsize(file_path) > 1024 * 1024: # Wait for at least 1MB
                logger.info(f"File {file_path} is ready for streaming.")
                return file_path

            status = handle.status()
            logger.debug(f"Waiting for file {file_index} in torrent {info_hash}. "
                         f"Status: {status.state}, Progress: {status.progress * 100:.2f}%")
            await asyncio.sleep(2)

        logger.error(f"Timeout waiting for file {file_index} in torrent {info_hash}.")
        return None

    def _get_info_hash_from_magnet(self, magnet_link):
        match = re.search(r'btih:([a-fA-F0-9]{40})', magnet_link)
        if match:
            return match.group(1).lower()
        match = re.search(r'btih:([a-zA-Z0-9]{32})', magnet_link)
        if match:
            # For base32 encoded info_hash
            import base64
            b32_hash = match.group(1)
            try:
                # Pad the base32 string if necessary
                b32_hash_padded = b32_hash.upper() + '=' * ((8 - len(b32_hash) % 8) % 8)
                decoded_hash = base64.b32decode(b32_hash_padded)
                return decoded_hash.hex()
            except Exception as e:
                logger.error(f"Error decoding base32 info_hash {b32_hash}: {e}")
                return None
        return None

torrent_client = TorrentClient()