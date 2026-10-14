package com.maxrave.data.mediaservice

import android.content.ContentValues
import android.content.Context
import android.media.MediaScannerConnection
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.provider.MediaStore
import com.arthenica.ffmpegkit.FFmpegKit
import com.arthenica.ffmpegkit.ReturnCode
import com.maxrave.domain.data.entities.SongEntity
import com.maxrave.domain.extension.now
import com.maxrave.domain.manager.DataStoreManager
import com.maxrave.domain.repository.StreamRepository
import com.maxrave.logger.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.lastOrNull
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.io.File
import java.io.FileOutputStream
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.ConcurrentHashMap

/**
 * Handles automatic background export of played tracks to the device's public
 * Music/SimpMusic directory as standard 320kbps MP3 files with embedded ID3v2.3 tags
 * and album art.
 */
class AutoMusicExporter(
    private val context: Context,
    private val dataStoreManager: DataStoreManager,
    private val streamRepository: StreamRepository,
) {
    companion object {
        private const val TAG = "AutoMusicExporter"
        private const val MUSIC_SUBDIR = "SimpMusic"
        private const val CONNECT_TIMEOUT_MS = 30_000
        private const val READ_TIMEOUT_MS = 60_000
    }

    private val mutex = Mutex()
    private val processedOrQueuedVideoIds = ConcurrentHashMap.newKeySet<String>()
    private var loggedTargetVolume = false

    suspend fun exportTrackIfEligible(song: SongEntity) = withContext(Dispatchers.IO) {
        val videoId = song.videoId.trim()
        if (videoId.isEmpty()) return@withContext

        // Check if user enabled auto-download for played songs
        val isEnabled = dataStoreManager.autoDownloadPlayedSongs.first() == DataStoreManager.Values.TRUE
        if (!isEnabled) {
            Logger.d(TAG, "Auto-download disabled by setting, skipping $videoId")
            return@withContext
        }

        // Avoid re-exporting tracks already processed in this session
        if (!processedOrQueuedVideoIds.add(videoId)) {
            Logger.d(TAG, "Track already queued/processed in this session: $videoId")
            return@withContext
        }

        mutex.withLock {
            try {
                processTrackExport(song)
            } catch (e: Exception) {
                Logger.e(TAG, "Error exporting track ${song.title}: ${e.message}")
                e.printStackTrace()
            }
        }
    }

    private suspend fun processTrackExport(song: SongEntity) {
        val artist = song.artistName?.joinToString(", ")?.ifBlank { "Unknown Artist" } ?: "Unknown Artist"
        val title = song.title.ifBlank { "Unknown Title" }
        val album = song.albumName?.ifBlank { title } ?: title

        val baseFileName = sanitizeFileName("$artist - $title").ifBlank { "track_${song.videoId}" }
        val targetFileName = "$baseFileName.mp3"

        // Check if file already exists in Music/SimpMusic
        if (checkFileExists(targetFileName)) {
            Logger.i(TAG, "Track already exists in Music/$MUSIC_SUBDIR: $targetFileName. Skipping download.")
            return
        }

        Logger.i(TAG, "Starting auto-export for: $targetFileName (videoId: ${song.videoId})")

        // 1. Resolve audio stream URL
        var audioUrl: String? = null
        streamRepository.getNewFormat(song.videoId).firstOrNull()?.let { format ->
            val candidate = format.audioUrl
            if (candidate != null && format.expiredTime > now()) {
                val is403 = streamRepository.is403Url(candidate).firstOrNull() != false
                if (!is403) {
                    audioUrl = candidate
                }
            }
        }

        if (audioUrl == null) {
            audioUrl = streamRepository.getStream(
                dataStoreManager = dataStoreManager,
                videoId = song.videoId,
                isDownloading = true,
                isVideo = false,
            ).lastOrNull()
        }

        if (audioUrl.isNullOrEmpty()) {
            Logger.e(TAG, "Could not resolve audio stream URL for ${song.videoId}")
            return
        }

        // 2. Prepare temporary directory and files
        val exportCacheDir = File(context.cacheDir, "auto_export").apply { mkdirs() }
        val tempRawAudio = File(exportCacheDir, "${song.videoId}_raw.tmp")
        val tempCoverJpg = File(exportCacheDir, "${song.videoId}_cover.jpg")
        val tempEncodedMp3 = File(exportCacheDir, "${song.videoId}_tagged.mp3")

        try {
            if (tempRawAudio.exists()) tempRawAudio.delete()
            if (tempCoverJpg.exists()) tempCoverJpg.delete()
            if (tempEncodedMp3.exists()) tempEncodedMp3.delete()

            // 3. Download raw audio stream via HttpURLConnection
            Logger.d(TAG, "Downloading raw audio stream for ${song.title}...")
            val downloadedAudio = downloadToFile(audioUrl, tempRawAudio)
            if (!downloadedAudio) {
                Logger.e(TAG, "Failed downloading raw audio for ${song.title}")
                return
            }

            // 4. Download album cover art thumbnail if available
            var hasThumbnail = false
            val thumbUrl = song.thumbnails?.let {
                Regex("=w\\d+-h\\d+").replace(it, "=w800-h800")
            } ?: "http://i.ytimg.com/vi/${song.videoId}/hqdefault.jpg"

            try {
                Logger.d(TAG, "Downloading cover art: $thumbUrl")
                hasThumbnail = downloadToFile(thumbUrl, tempCoverJpg) && tempCoverJpg.length() > 0
            } catch (e: Exception) {
                Logger.w(TAG, "Failed downloading thumbnail: ${e.message}. Proceeding without cover art.")
                hasThumbnail = false
            }

            // 5. Run FFmpeg to encode 320kbps MP3 with ID3v2.3 tags and optional embedded cover art
            val safeTitle = sanitizeMetadata(title)
            val safeArtist = sanitizeMetadata(artist)
            val safeAlbum = sanitizeMetadata(album)

            val ffmpegCommand = if (hasThumbnail && tempCoverJpg.exists()) {
                listOf(
                    "-y",
                    "-i", "\"${tempRawAudio.absolutePath}\"",
                    "-i", "\"${tempCoverJpg.absolutePath}\"",
                    "-map", "0:a",
                    "-map", "1:v",
                    "-c:a", "libmp3lame",
                    "-b:a", "320k",
                    "-c:v", "copy",
                    "-id3v2_version", "3",
                    "-metadata", "title=\"$safeTitle\"",
                    "-metadata", "artist=\"$safeArtist\"",
                    "-metadata", "album=\"$safeAlbum\"",
                    "-disposition:v:0", "attached_pic",
                    "\"${tempEncodedMp3.absolutePath}\"",
                ).joinToString(" ")
            } else {
                listOf(
                    "-y",
                    "-i", "\"${tempRawAudio.absolutePath}\"",
                    "-c:a", "libmp3lame",
                    "-b:a", "320k",
                    "-id3v2_version", "3",
                    "-metadata", "title=\"$safeTitle\"",
                    "-metadata", "artist=\"$safeArtist\"",
                    "-metadata", "album=\"$safeAlbum\"",
                    "\"${tempEncodedMp3.absolutePath}\"",
                ).joinToString(" ")
            }

            Logger.d(TAG, "Executing FFmpeg: $ffmpegCommand")
            val session = FFmpegKit.execute(ffmpegCommand)

            if (!ReturnCode.isSuccess(session.returnCode) || !tempEncodedMp3.exists() || tempEncodedMp3.length() == 0L) {
                Logger.e(TAG, "FFmpeg encoding failed: code=${session.returnCode}, state=${session.state}")
                return
            }

            // 6. Save resulting MP3 to MediaStore / Music/SimpMusic
            saveToMediaStore(
                tempMp3 = tempEncodedMp3,
                fileName = targetFileName,
                title = title,
                artist = artist,
                album = album,
            )

            Logger.i(TAG, "Successfully auto-downloaded and saved: $targetFileName to Music/$MUSIC_SUBDIR")
        } finally {
            // Clean up scratch files
            if (tempRawAudio.exists()) tempRawAudio.delete()
            if (tempCoverJpg.exists()) tempCoverJpg.delete()
            if (tempEncodedMp3.exists()) tempEncodedMp3.delete()
        }
    }

    private fun downloadToFile(urlString: String, targetFile: File): Boolean {
        var connection: HttpURLConnection? = null
        return try {
            val url = URL(urlString)
            connection = (url.openConnection() as HttpURLConnection).apply {
                connectTimeout = CONNECT_TIMEOUT_MS
                readTimeout = READ_TIMEOUT_MS
                requestMethod = "GET"
                instanceFollowRedirects = true
                setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                setRequestProperty("Accept", "*/*")
                setRequestProperty("Connection", "keep-alive")
            }
            connection.connect()
            val code = connection.responseCode
            if (code in 200..299) {
                connection.inputStream.buffered(65536).use { input ->
                    FileOutputStream(targetFile).buffered(65536).use { output ->
                        input.copyTo(output, bufferSize = 65536)
                    }
                }
                targetFile.length() > 0
            } else {
                Logger.e(TAG, "HTTP error $code downloading from $urlString")
                false
            }
        } catch (e: Exception) {
            Logger.e(TAG, "Error downloading from $urlString: ${e.message}")
            false
        } finally {
            try {
                connection?.disconnect()
            } catch (_: Exception) {}
        }
    }

    /**
     * Resolves where exports are written: a removable secondary volume (SD card) when one is
     * mounted, otherwise the primary emulated volume. Returns the MediaStore collection URI for
     * that volume plus its absolute filesystem root, or null when the API is too old to ask.
     */
    private fun resolveExportVolume(): Pair<Uri, String>? {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.Q) return null
        return runCatching {
            val volumeNames = MediaStore.getExternalVolumeNames(context)
            val volumeRoots = context.getExternalFilesDirs(null)
            val primaryRoot = context.getExternalFilesDir(null) ?: Environment.getExternalStorageDirectory()
            // getExternalFilesDirs returns one entry per mounted volume in the same order as
            // getExternalVolumeNames; skip the one that IS the primary emulated storage.
            volumeRoots.forEachIndexed { index, root ->
                if (root == null || root == primaryRoot) return@forEachIndexed
                val volumeName = volumeNames.filterIsInstance<String>().getOrNull(index) ?: return@forEachIndexed
                val absoluteRoot = root.absolutePath.trimEnd('/')
                // <root>/Android/data/<pkg>/files -> strip three levels to get <root>
                val storageRoot = root.parentFile?.parentFile?.parentFile ?: return@forEachIndexed
                val isMounted = Environment.getExternalStorageState(storageRoot) == Environment.MEDIA_MOUNTED
                val target = absoluteRoot.substringBefore("/Android/data")
                if (isMounted && target.isNotBlank()) {
                    return@runCatching MediaStore.Audio.Media.getContentUri(volumeName) to target
                }
            }
            null
        }.getOrNull()
    }

    private fun checkFileExists(fileName: String): Boolean {
        return runCatching {
            val resolver = context.contentResolver
            val isScopedStorage = Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q
            if (isScopedStorage) {
                val (collectionUri, _) =
                    resolveExportVolume() ?: (MediaStore.Audio.Media.EXTERNAL_CONTENT_URI to null)
                val projection = arrayOf(MediaStore.Audio.Media._ID, MediaStore.Audio.Media.DISPLAY_NAME)
                val selection = "${MediaStore.Audio.Media.DISPLAY_NAME} = ? AND ${MediaStore.Audio.Media.RELATIVE_PATH} LIKE ?"
                val selectionArgs = arrayOf(fileName, "%Music/$MUSIC_SUBDIR%")
                resolver.query(
                    collectionUri,
                    projection,
                    selection,
                    selectionArgs,
                    null,
                )?.use { cursor ->
                    cursor.count > 0
                } ?: false
            } else {
                val legacyFile = File(
                    Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_MUSIC),
                    "$MUSIC_SUBDIR/$fileName",
                )
                legacyFile.exists() && legacyFile.length() > 0
            }
        }.getOrDefault(false)
    }

    private fun saveToMediaStore(
        tempMp3: File,
        fileName: String,
        title: String,
        artist: String,
        album: String,
    ) {
        val resolver = context.contentResolver
        val isScopedStorage = Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q

        if (isScopedStorage) {
            val (collectionUri, volumeRoot) =
                resolveExportVolume() ?: (MediaStore.Audio.Media.EXTERNAL_CONTENT_URI to null)
            if (!loggedTargetVolume) {
                Logger.i(TAG, "Auto-export target volume: ${volumeRoot ?: "primary"}")
                loggedTargetVolume = true
            }

            val values = ContentValues().apply {
                put(MediaStore.Audio.Media.DISPLAY_NAME, fileName)
                put(MediaStore.Audio.Media.TITLE, title)
                put(MediaStore.Audio.Media.ARTIST, artist)
                put(MediaStore.Audio.Media.ALBUM, album)
                put(MediaStore.Audio.Media.MIME_TYPE, "audio/mpeg")
                put(MediaStore.Audio.Media.RELATIVE_PATH, "${Environment.DIRECTORY_MUSIC}/$MUSIC_SUBDIR")
                put(MediaStore.Audio.Media.IS_PENDING, 1)
            }

            val uri = resolver.insert(collectionUri, values)
            if (uri != null) {
                resolver.openOutputStream(uri)?.use { output ->
                    tempMp3.inputStream().use { input ->
                        input.copyTo(output)
                    }
                }
                values.clear()
                values.put(MediaStore.Audio.Media.IS_PENDING, 0)
                resolver.update(uri, values, null, null)

                // Trigger media scanner on modern Android
                runCatching {
                    val basePath =
                        volumeRoot
                            ?: Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_MUSIC).path
                    MediaScannerConnection.scanFile(
                        context,
                        arrayOf("$basePath/$MUSIC_SUBDIR/$fileName"),
                        arrayOf("audio/mpeg"),
                        null,
                    )
                }
            } else {
                Logger.e(TAG, "Failed to insert audio entry into MediaStore for $fileName")
            }
        } else {
            val dir = File(
                Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_MUSIC),
                MUSIC_SUBDIR,
            ).apply { mkdirs() }
            val destFile = File(dir, fileName)
            tempMp3.copyTo(destFile, overwrite = true)

            MediaScannerConnection.scanFile(
                context,
                arrayOf(destFile.absolutePath),
                arrayOf("audio/mpeg"),
                null,
            )
        }
    }

    private fun sanitizeFileName(name: String): String {
        return name
            .replace(Regex("""[\\/:*?"<>|]"""), "_")
            .replace(Regex("""\s+"""), " ")
            .trim()
    }

    private fun sanitizeMetadata(text: String): String {
        return text
            .replace("\"", "\\\"")
            .replace("\r", " ")
            .replace("\n", " ")
            .trim()
    }
}
