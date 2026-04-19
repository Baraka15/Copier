#!/usr/bin/env python3
"""
Telegram Stealth Copier - Advanced Production Version (Single File)
Features:
- Stealth mode (removes "Forwarded from" by re-uploading media)
- Album/grouped media preservation
- Adaptive flood control with jitter
- Asyncio queue for ordered processing
- Multi-source chat monitoring
- Automatic reconnection on network failures
- Structured logging with rotation
- Graceful shutdown on SIGTERM/SIGINT
- Configuration via environment variables
"""

import asyncio
import io
import os
import random
import signal
import sys
import tempfile
from collections import deque
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Optional, Union
import logging

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
from telethon.sessions import StringSession
from telethon.tl.types import (
    DocumentAttributeFilename,
    Message,
    MessageMediaDocument,
    MessageMediaPhoto,
)

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# Logging Setup
# =============================================================================
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Rotating file handler (10 MB max, keep 5 backups)
    file_handler = RotatingFileHandler(
        LOG_DIR / "copier.log",
        maxBytes=10*1024*1024,
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

logger = setup_logger("Copier")

# =============================================================================
# Adaptive Flood Controller
# =============================================================================
class FloodController:
    """Adaptive rate limiter with jitter to avoid detection."""
    
    def __init__(self, max_requests_per_minute: int = 20):
        self.max_requests = max_requests_per_minute
        self.window = timedelta(minutes=1)
        self.request_times = deque()
        self.base_delay = 3.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = datetime.now()
            # Remove timestamps older than the window
            while self.request_times and now - self.request_times[0] > self.window:
                self.request_times.popleft()

            if len(self.request_times) >= self.max_requests:
                oldest = self.request_times[0]
                wait_seconds = (oldest + self.window - now).total_seconds() + 0.5
                if wait_seconds > 0:
                    logger.debug(f"Rate limit approaching, waiting {wait_seconds:.1f}s")
                    await asyncio.sleep(wait_seconds)
                    return await self.acquire()

            # Add jitter to appear more human
            jitter = random.uniform(0.5, 2.0)
            await asyncio.sleep(self.base_delay + jitter)
            self.request_times.append(datetime.now())

# =============================================================================
# Stealth Media Processor
# =============================================================================
class StealthMediaProcessor:
    """Handles downloading and re-uploading media to remove 'Forwarded from' header."""
    
    def __init__(self, client: TelegramClient, temp_dir: str = "temp_media"):
        self.client = client
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(exist_ok=True)

    async def process_and_send(
        self, 
        message: Message, 
        destination: int,
        caption: Optional[str] = None
    ) -> Optional[Message]:
        """Main entry point: download media, re-upload to destination."""
        try:
            if message.photo:
                return await self._handle_photo(message, destination, caption)
            elif message.video:
                return await self._handle_video(message, destination, caption)
            elif message.document:
                return await self._handle_document(message, destination, caption)
            elif message.audio:
                return await self._handle_audio(message, destination, caption)
            elif message.voice:
                return await self._handle_voice(message, destination, caption)
            else:
                # Fallback for text-only messages
                return await self.client.send_message(destination, message.text or "")
        except Exception as e:
            logger.error(f"Media processing failed: {e}, falling back to forward")
            # Fallback to regular forward
            try:
                return await self.client.forward_messages(destination, message)
            except Exception:
                return None

    async def _handle_photo(self, msg: Message, dest: int, caption: Optional[str]):
        temp_path = self.temp_dir / f"photo_{msg.id}.jpg"
        try:
            await self.client.download_media(msg.media, file=str(temp_path))
            return await self.client.send_file(
                dest,
                file=str(temp_path),
                caption=caption or msg.text,
                parse_mode="html"
            )
        finally:
            self._cleanup(temp_path)

    async def _handle_video(self, msg: Message, dest: int, caption: Optional[str]):
        temp_path = self.temp_dir / f"video_{msg.id}.mp4"
        try:
            await self.client.download_media(msg.media, file=str(temp_path))
            attributes = msg.video.attributes if msg.video else []
            return await self.client.send_file(
                dest,
                file=str(temp_path),
                caption=caption or msg.text,
                supports_streaming=True,
                attributes=attributes,
                parse_mode="html"
            )
        finally:
            self._cleanup(temp_path)

    async def _handle_document(self, msg: Message, dest: int, caption: Optional[str]):
        filename = None
        for attr in msg.document.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                filename = attr.file_name
                break
        if not filename:
            ext = msg.document.mime_type.split('/')[-1] if msg.document.mime_type else 'bin'
            filename = f"file_{msg.id}.{ext}"
        
        temp_path = self.temp_dir / filename
        try:
            await self.client.download_media(msg.media, file=str(temp_path))
            return await self.client.send_file(
                dest,
                file=str(temp_path),
                caption=caption or msg.text,
                force_document=True,
                parse_mode="html"
            )
        finally:
            self._cleanup(temp_path)

    async def _handle_audio(self, msg: Message, dest: int, caption: Optional[str]):
        temp_path = self.temp_dir / f"audio_{msg.id}.mp3"
        try:
            await self.client.download_media(msg.media, file=str(temp_path))
            return await self.client.send_file(
                dest,
                file=str(temp_path),
                caption=caption or msg.text,
                parse_mode="html"
            )
        finally:
            self._cleanup(temp_path)

    async def _handle_voice(self, msg: Message, dest: int, caption: Optional[str]):
        temp_path = self.temp_dir / f"voice_{msg.id}.ogg"
        try:
            await self.client.download_media(msg.media, file=str(temp_path))
            return await self.client.send_file(
                dest,
                file=str(temp_path),
                caption=caption or msg.text,
                voice_note=True,
                parse_mode="html"
            )
        finally:
            self._cleanup(temp_path)

    def _cleanup(self, path: Path):
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass

# =============================================================================
# Message Copier Core
# =============================================================================
class MessageCopier:
    def __init__(
        self,
        client: TelegramClient,
        source_chats: List[int],
        dest_chat: int,
        stealth_mode: bool = True,
        flood_controller: Optional[FloodController] = None
    ):
        self.client = client
        self.source_chats = source_chats
        self.dest_chat = dest_chat
        self.stealth_mode = stealth_mode
        self.flood = flood_controller or FloodController()
        self.media_processor = StealthMediaProcessor(client) if stealth_mode else None
        self.queue = asyncio.Queue()
        self.processing_task: Optional[asyncio.Task] = None
        self.album_buffer: Dict[int, List[Message]] = {}
        self.album_timer: Optional[asyncio.Task] = None

    async def start(self):
        """Start the queue processor and album timer."""
        self.processing_task = asyncio.create_task(self._process_queue())
        self.album_timer = asyncio.create_task(self._album_flush_loop())
        logger.info(f"Message copier started (stealth={self.stealth_mode})")

    async def stop(self):
        """Gracefully stop the processor."""
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
        if self.album_timer:
            self.album_timer.cancel()
        logger.info("Message copier stopped")

    async def handle_new_message(self, event: events.NewMessage.Event):
        """Event handler for incoming messages."""
        if event.message.chat_id in self.source_chats:
            # Handle grouped media (albums)
            if event.message.grouped_id:
                group_id = event.message.grouped_id
                if group_id not in self.album_buffer:
                    self.album_buffer[group_id] = []
                self.album_buffer[group_id].append(event.message)
                logger.debug(f"Buffered album message {event.message.id} (group {group_id})")
            else:
                await self.queue.put(event.message)
                logger.debug(f"Queued message {event.message.id}")

    async def _album_flush_loop(self):
        """Periodically flush completed albums."""
        while True:
            try:
                await asyncio.sleep(2)  # Wait for album to complete
                now = datetime.now()
                to_remove = []
                for group_id, messages in self.album_buffer.items():
                    # Flush if messages are older than 3 seconds (album complete)
                    if messages and (now - messages[0].date).total_seconds() > 3:
                        # Sort by ID to maintain order
                        messages.sort(key=lambda m: m.id)
                        await self.queue.put(messages)
                        to_remove.append(group_id)
                        logger.info(f"Flushed album with {len(messages)} items (group {group_id})")
                for gid in to_remove:
                    del self.album_buffer[gid]
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Album flush error: {e}")

    async def _process_queue(self):
        """Background task that processes messages with rate limiting."""
        while True:
            try:
                item = await self.queue.get()
                
                # Check if it's an album (list of messages)
                if isinstance(item, list):
                    await self._handle_album(item)
                else:
                    await self._handle_single_message(item)
                    
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Queue processing error: {e}")
                await asyncio.sleep(5)  # Backoff on error

    async def _handle_single_message(self, message: Message):
        await self.flood.acquire()
        
        if self.stealth_mode and (message.media or message.photo):
            await self.media_processor.process_and_send(
                message=message,
                destination=self.dest_chat,
                caption=message.text
            )
        else:
            await self.client.forward_messages(self.dest_chat, message)
        
        logger.info(f"Copied message {message.id}")

    async def _handle_album(self, messages: List[Message]):
        """Handle media group as a single album."""
        if not self.stealth_mode:
            # Simple forward for albums when not in stealth mode
            await self.flood.acquire()
            msg_ids = [m.id for m in messages]
            await self.client.forward_messages(self.dest_chat, messages[0].chat_id, msg_ids)
            logger.info(f"Forwarded album with {len(messages)} items")
            return

        # Stealth mode: download and re-upload as album
        album_files = []
        captions = []
        for msg in messages:
            if msg.photo or msg.video:
                ext = "jpg" if msg.photo else "mp4"
                temp_path = self.media_processor.temp_dir / f"album_{msg.id}.{ext}"
                try:
                    await self.client.download_media(msg.media, file=str(temp_path))
                    album_files.append(str(temp_path))
                    captions.append(msg.text if msg == messages[0] else "")
                except Exception as e:
                    logger.error(f"Failed to download album media {msg.id}: {e}")
        
        if album_files:
            await self.flood.acquire()
            try:
                await self.client.send_file(
                    self.dest_chat,
                    file=album_files,
                    caption=captions[0] if captions else "",
                    parse_mode="html"
                )
                logger.info(f"Stealth-copied album with {len(album_files)} items")
            except Exception as e:
                logger.error(f"Album send failed: {e}")
            finally:
                # Cleanup temp files
                for path in album_files:
                    Path(path).unlink(missing_ok=True)

# =============================================================================
# Main Client Manager
# =============================================================================
class CopierClient:
    def __init__(
        self,
        api_id: int,
        api_hash: str,
        phone: str,
        source_chats: List[int],
        dest_chat: int,
        session_string: Optional[str] = None,
        stealth_mode: bool = True
    ):
        self.session = StringSession(session_string) if session_string else StringSession()
        self.client = TelegramClient(self.session, api_id, api_hash)
        self.phone = phone
        self.source_chats = source_chats
        self.dest_chat = dest_chat
        self.stealth_mode = stealth_mode
        self.copier: Optional[MessageCopier] = None

    async def start(self):
        """Connect and authenticate, then start copier."""
        await self.client.start(phone=self.phone)
        logger.info("Client authenticated")

        # Print/save session string for reuse
        session_str = self.session.save()
        logger.info("=== SESSION STRING (save this for future runs) ===")
        print(f"\nSESSION_STRING={session_str}\n")
        logger.info("=== END SESSION STRING ===")

        self.copier = MessageCopier(
            client=self.client,
            source_chats=self.source_chats,
            dest_chat=self.dest_chat,
            stealth_mode=self.stealth_mode
        )
        await self.copier.start()

        # Register event handler
        self.client.add_event_handler(
            self.copier.handle_new_message,
            events.NewMessage(chats=self.source_chats)
        )

        logger.info(f"Monitoring {len(self.source_chats)} source chats")

    async def run_forever(self):
        """Run with automatic reconnection on disconnect."""
        while True:
            try:
                if not self.client.is_connected():
                    await self.client.connect()
                await self.client.run_until_disconnected()
            except (ConnectionError, OSError) as e:
                logger.warning(f"Connection lost: {e}. Reconnecting in 10s...")
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}. Restarting in 30s...")
                await asyncio.sleep(30)

    async def stop(self):
        """Graceful shutdown."""
        if self.copier:
            await self.copier.stop()
        await self.client.disconnect()
        logger.info("Client stopped")

# =============================================================================
# Helper Functions and Entry Point
# =============================================================================
def parse_chat_list(env_value: str) -> List[int]:
    """Parse comma-separated chat IDs from environment variable."""
    if not env_value:
        return []
    return [int(chat_id.strip()) for chat_id in env_value.split(",") if chat_id.strip()]

async def shutdown(signal, loop, client: CopierClient):
    """Handle graceful shutdown on SIGTERM/SIGINT."""
    logger.info(f"Received signal {signal.name}, shutting down...")
    await client.stop()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def main():
    # Load configuration from environment
    API_ID = int(os.getenv("API_ID", "0"))
    API_HASH = os.getenv("API_HASH", "")
    PHONE = os.getenv("PHONE_NUMBER", "")
    SOURCE_CHATS = parse_chat_list(os.getenv("SOURCE_CHATS", ""))
    DEST_CHAT = int(os.getenv("DEST_CHAT", "0"))
    SESSION_STRING = os.getenv("SESSION_STRING", "")
    STEALTH_MODE = os.getenv("STEALTH_MODE", "true").lower() == "true"

    if not all([API_ID, API_HASH, PHONE, SOURCE_CHATS, DEST_CHAT]):
        logger.error("Missing required environment variables. Check your .env file or environment settings.")
        logger.error(f"API_ID: {bool(API_ID)}, API_HASH: {bool(API_HASH)}, PHONE: {bool(PHONE)}, SOURCE_CHATS: {bool(SOURCE_CHATS)}, DEST_CHAT: {bool(DEST_CHAT)}")
        return

    client = CopierClient(
        api_id=API_ID,
        api_hash=API_HASH,
        phone=PHONE,
        source_chats=SOURCE_CHATS,
        dest_chat=DEST_CHAT,
        session_string=SESSION_STRING,
        stealth_mode=STEALTH_MODE
    )

    # Register signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown(s, loop, client))
        )

    try:
        await client.start()
        await client.run_forever()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
