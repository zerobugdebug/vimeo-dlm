package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/yapingcat/gomedia/go-mp4"
)

// Segment represents a media segment in the stream.
type Segment struct {
	Start float64 `json:"start"`
	End   float64 `json:"end"`
	URL   string  `json:"url"`
}

type VideoStream struct {
	Width       int       `json:"width"`
	Height      int       `json:"height"`
	ID          string    `json:"id"`
	BaseURL     string    `json:"base_url"`
	Bitrate     int       `json:"bitrate"`
	AvgBitrate  int       `json:"avg_bitrate"`
	Codecs      string    `json:"codecs"`
	InitSegment string    `json:"init_segment"`
	Segments    []Segment `json:"segments"`
}

type AudioStream struct {
	ID          string    `json:"id"`
	BaseURL     string    `json:"base_url"`
	Bitrate     int       `json:"bitrate"`
	Channels    int       `json:"channels"`
	SampleRate  int       `json:"sample_rate"`
	InitSegment string    `json:"init_segment"`
	Segments    []Segment `json:"segments"`
}

type PlaylistResponse struct {
	ClipID  string        `json:"clip_id"`
	BaseURL string        `json:"base_url"`
	Video   []VideoStream `json:"video"`
	Audio   []AudioStream `json:"audio"`
}

type Vimeo struct {
	playlistURL string
	outputPath  string
	response    *PlaylistResponse
	mainBase    string
	concurrency int
}

// fetchVimeoJWT calls https://vimeo.com/_next/jwt to obtain a JWT token.
func fetchVimeoJWT(ctx context.Context) (string, error) {
	log.Debug().Msg("Starting fetchVimeoJWT() ...")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://vimeo.com/_next/jwt", nil)
	if err != nil {
		return "", fmt.Errorf("failed to build JWT request: %w", err)
	}

	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	log.Debug().Msg("Sending request to https://vimeo.com/_next/jwt")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error().Err(err).Msg("Failed to request JWT")
		return "", fmt.Errorf("failed to request JWT: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Error().Int("status_code", resp.StatusCode).Msg("Unexpected HTTP status for JWT")
		return "", fmt.Errorf("unexpected HTTP status for JWT: %d", resp.StatusCode)
	}

	var data struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		log.Error().Err(err).Msg("Failed to parse JWT JSON")
		return "", fmt.Errorf("failed to parse JWT JSON: %w", err)
	}
	if data.Token == "" {
		log.Error().Msg("JWT token not found in response")
		return "", errors.New("JWT token not found in response")
	}

	log.Debug().Str("token_prefix", data.Token[:15]).Msg("Successfully fetched JWT")
	return data.Token, nil
}

// fetchVimeoConfigURL gets the config_url from https://api.vimeo.com/videos/<video_id>

func fetchVimeoConfigURL(ctx context.Context, videoID, jwt string) (string, error) {
	apiURL := fmt.Sprintf("https://api.vimeo.com/videos/%s?fields=config_url", videoID)
	log.Debug().Str("url", apiURL).Msg("fetchVimeoConfigURL")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to build config_url request: %w", err)
	}
	req.Header.Set("Authorization", "jwt "+jwt)

	log.Debug().Msg("Sending request to get config_url ...")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to request config_url: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected HTTP status for config_url: %d", resp.StatusCode)
	}

	var data struct {
		ConfigURL string `json:"config_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", fmt.Errorf("failed to parse config_url JSON: %w", err)
	}
	if data.ConfigURL == "" {
		return "", errors.New("config_url not found in response")
	}

	log.Debug().Str("config_url", data.ConfigURL).Msg("Received config_url")
	return data.ConfigURL, nil
}

// fetchAVCPlaylistURL calls the config_url to get the "request.files.dash.cdns.<default_cdn>.avc_url"
func fetchAVCPlaylistURL(ctx context.Context, configURL string) (string, error) {
	log.Debug().Str("configURL", configURL).Msg("fetchAVCPlaylistURL")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, configURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to build configURL request: %w", err)
	}

	log.Debug().Msg("Sending request to configURL for AVC ...")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to request configURL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected HTTP status from config_url: %d", resp.StatusCode)
	}

	var data struct {
		Request struct {
			Files struct {
				Dash struct {
					DefaultCDN string `json:"default_cdn"`
					CDNs       map[string]struct {
						AVCURL string `json:"avc_url"`
					} `json:"cdns"`
				} `json:"dash"`
			} `json:"files"`
		} `json:"request"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", fmt.Errorf("failed to parse config JSON: %w", err)
	}

	defaultCDN := data.Request.Files.Dash.DefaultCDN
	if defaultCDN == "" {
		return "", errors.New("default_cdn not found in config")
	}
	cdn, ok := data.Request.Files.Dash.CDNs[defaultCDN]
	if !ok {
		return "", fmt.Errorf("CDN '%s' not found in dash.cdns", defaultCDN)
	}
	if cdn.AVCURL == "" {
		return "", errors.New("avc_url not found in config")
	}

	log.Debug().Str("avc_url", cdn.AVCURL).Msg("Received AVC URL")
	return cdn.AVCURL, nil
}

// resolvePlaylistURLFromVideoID resolves the final playlist URL

func resolvePlaylistURLFromVideoID(videoID string) (string, error) {
	log.Debug().Str("videoID", videoID).Msg("resolvePlaylistURLFromVideoID")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jwt, err := fetchVimeoJWT(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch Vimeo JWT: %w", err)
	}

	configURL, err := fetchVimeoConfigURL(ctx, videoID, jwt)
	if err != nil {
		return "", fmt.Errorf("failed to fetch config_url: %w", err)
	}

	avcURL, err := fetchAVCPlaylistURL(ctx, configURL)
	if err != nil {
		return "", fmt.Errorf("failed to fetch avc_url: %w", err)
	}

	return avcURL, nil
}

func (v *Vimeo) SendRequest() error {
	log.Debug().Str("url", v.playlistURL).Msg("Fetching playlist")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.playlistURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request for playlistURL: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to GET playlist URL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d for playlist JSON", resp.StatusCode)
	}

	var pr PlaylistResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return fmt.Errorf("failed to decode playlist JSON: %w", err)
	}

	v.response = &pr
	log.Info().
		Str("clipID", pr.ClipID).
		Str("baseURL", pr.BaseURL).
		Msg("Playlist JSON successfully retrieved")
	return nil
}

func (v *Vimeo) ParsePlaylist() error {
	if v.response == nil {
		return errors.New("no response data available")
	}

	log.Debug().Msg("Sorting video streams by descending bitrate")

	sort.Slice(v.response.Video, func(i, j int) bool {
		return v.response.Video[i].Bitrate > v.response.Video[j].Bitrate
	})

	log.Debug().Msg("Sorting audio streams by descending bitrate")

	sort.Slice(v.response.Audio, func(i, j int) bool {
		return v.response.Audio[i].Bitrate > v.response.Audio[j].Bitrate
	})

	baseURL, err := url.Parse(v.playlistURL)
	if err != nil {
		log.Error().Err(err).Msg("Failed to parse playlist URL")
		return fmt.Errorf("failed to parse playlistURL: %w", err)
	}
	ref, err := url.Parse(v.response.BaseURL)
	if err != nil {
		log.Error().Err(err).Msg("Failed to parse response base URL")
		return fmt.Errorf("failed to parse response.BaseURL: %w", err)
	}
	resolved := baseURL.ResolveReference(ref).String()
	v.mainBase = resolved

	log.Debug().Str("main_base", v.mainBase).Msg("Base URL resolved successfully")

	return nil
}

func (v *Vimeo) BestVideoStream() *VideoStream {
	if len(v.response.Video) == 0 {
		return nil
	}
	best := &v.response.Video[0]
	log.Debug().
		Str("id", best.ID).
		Int("width", best.Width).
		Int("height", best.Height).
		Int("bitrate", best.Bitrate).
		Msg("Best video stream selected")
	return best
}

func (v *Vimeo) BestAudioStream() *AudioStream {
	if len(v.response.Audio) == 0 {
		return nil
	}
	best := &v.response.Audio[0]
	log.Debug().
		Str("id", best.ID).
		Int("channels", best.Channels).
		Int("bitrate", best.Bitrate).
		Msg("Best audio stream selected")
	return best
}

func buildSegmentURL(base, segURL string) (string, error) {
	segBase, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("failed to parse base URL: %w", err)
	}
	ref, err := url.Parse(segURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse segment URL: %w", err)
	}
	return segBase.ResolveReference(ref).String(), nil
}

func (v *Vimeo) downloadSegments(segmentList []Segment, initBase64 string, outputFileName string) error {
	if len(segmentList) == 0 {
		return fmt.Errorf("segmentList is empty, cannot download => %s", outputFileName)
	}

	outPath := filepath.Join(v.outputPath, outputFileName)
	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", outPath, err)
	}
	defer f.Close()

	initData, err := base64.StdEncoding.DecodeString(initBase64)
	if err != nil {
		return fmt.Errorf("failed to decode init segment: %w", err)
	}
	log.Debug().
		Int("length", len(initData)).
		Str("file", outputFileName).
		Msg("Writing init segment")

	if _, err := f.Write(initData); err != nil {
		return fmt.Errorf("failed to write init segment: %w", err)
	}
	log.Info().Str("file", outputFileName).Msg("Wrote init segment")

	concurrency := v.concurrency
	if concurrency < 1 {
		concurrency = 1
	}
	log.Debug().
		Int("concurrency", concurrency).
		Str("file", outputFileName).
		Msg("Starting concurrent downloads")

	results := make([][]byte, len(segmentList))
	errorsChan := make(chan error, len(segmentList))

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	for idx, seg := range segmentList {
		wg.Add(1)
		go func(i int, s Segment) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			segURL, errBuild := buildSegmentURL(v.mainBase, s.URL)
			if errBuild != nil {
				errorsChan <- fmt.Errorf("failed to build segment URL for idx %d: %w", i, errBuild)
				return
			}

			log.Debug().
				Int("index", i).
				Str("url", segURL).
				Msg("Downloading segment")

			data, errDl := downloadSegment(segURL)
			if errDl != nil {
				errorsChan <- fmt.Errorf("failed to download segment idx %d: %w", i, errDl)
				return
			}
			results[i] = data
			log.Debug().
				Int("index", i).
				Int("size", len(data)).
				Msg("Segment downloaded")
		}(idx, seg)
	}

	wg.Wait()
	close(errorsChan)

	for e := range errorsChan {
		if e != nil {
			return e
		}
	}

	for i, segData := range results {
		log.Debug().
			Int("index", i).
			Int("length", len(segData)).
			Msg("Writing segment to file")
		if _, err := f.Write(segData); err != nil {
			return fmt.Errorf("failed writing segment idx %d to file: %w", i, err)
		}
	}

	log.Info().
		Int("segments", len(segmentList)).
		Str("file", outputFileName).
		Msg("Downloaded and concatenated segments")
	return nil
}

func downloadSegment(segURL string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, segURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	log.Debug().Str("url", segURL).Msg("Downloading segment")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to GET segment: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("segment GET returned status %d for URL: %s", resp.StatusCode, segURL)
	}

	return io.ReadAll(resp.Body)
}

func main() {
	// Configure zerolog
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "2006-01-02 15:04:05.000",
		NoColor:    false,
	}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()

	videoIDFlag := flag.String("video-id", "", "Vimeo video ID (optionally with :hash or /hash) e.g. 12345:abcd")
	playlistURLFlag := flag.String("url", "", "Direct playlist JSON URL (optional)")
	outputFlag := flag.String("output", ".", "Output directory")
	threadsFlag := flag.Int("threads", 6, "Number of threads for segment download (1 = sequential)")
	videoPath := flag.String("video", "best_video.mp4", "Path to best_video.mp4")
	audioPath := flag.String("audio", "best_audio.m4a", "Path to best_audio.m4a")
	outPath := flag.String("out", "result.mp4", "Path for the merged output file")
	flag.Parse()

	if *videoIDFlag == "" && *playlistURLFlag == "" {
		log.Fatal().Msg("Either --video <id> or --url <playlistURL> must be provided")
	}

	var finalPlaylistURL string
	if *playlistURLFlag != "" {
		finalPlaylistURL = *playlistURLFlag

		log.Info().Str("url", finalPlaylistURL).Msg("Using direct playlist URL")
	} else {

		raw := *videoIDFlag

		log.Debug().Str("video_id", raw).Msg("Processing video ID")

		var forAPI string
		if strings.Contains(raw, ":") {

			forAPI = raw
		} else if strings.Contains(raw, "/") {

			parts := strings.SplitN(raw, "/", 2)
			forAPI = parts[0] + ":" + parts[1]
		} else {

			forAPI = raw
		}

		log.Debug().Str("api_id", forAPI).Msg("Formatted ID for API")

		resolved, err := resolvePlaylistURLFromVideoID(forAPI)
		if err != nil {
			log.Fatal().Err(err).Str("video_id", raw).Msg("Could not resolve playlist URL")
		}
		finalPlaylistURL = resolved
		log.Info().Str("url", finalPlaylistURL).Msg("Resolved final playlist URL")
	}

	vimeo := &Vimeo{
		playlistURL: finalPlaylistURL,
		outputPath:  *outputFlag,
		concurrency: *threadsFlag,
	}

	if err := vimeo.SendRequest(); err != nil {
		log.Fatal().Err(err).Msg("Failed to get playlist JSON")
	}

	if err := vimeo.ParsePlaylist(); err != nil {
		log.Fatal().Err(err).Msg("Failed to parse playlist")
	}

	bestVideo := vimeo.BestVideoStream()
	bestAudio := vimeo.BestAudioStream()

	if bestVideo == nil && bestAudio == nil {
		log.Fatal().Msg("No video or audio streams found in the playlist JSON")
	}

	if bestVideo != nil {
		log.Info().
			Str("resolution", fmt.Sprintf("%dx%d", bestVideo.Width, bestVideo.Height)).
			Int("bitrate", bestVideo.Bitrate).
			Str("output", *videoPath).
			Msg("Downloading video stream")

		if err := vimeo.downloadSegments(bestVideo.Segments, bestVideo.InitSegment, *videoPath); err != nil {

			log.Fatal().Err(err).Msg("Failed to download video stream")
		}
	}

	if bestAudio != nil {
		log.Info().
			Int("channels", bestAudio.Channels).
			Int("bitrate", bestAudio.Bitrate).
			Str("output", *audioPath).
			Msg("Downloading audio stream")

		if err := vimeo.downloadSegments(bestAudio.Segments, bestAudio.InitSegment, *audioPath); err != nil {

			log.Fatal().Err(err).Msg("Failed to download audio stream")
		}
	}

	// Process MP4 files
	videoFile, err := os.Open(*videoPath)
	if err != nil {
		log.Fatal().Err(err).Str("path", *videoPath).Msg("Failed to open video file")
	}
	defer videoFile.Close()

	audioFile, err := os.Open(*audioPath)
	if err != nil {
		log.Fatal().Err(err).Str("path", *audioPath).Msg("Failed to open audio file")
	}
	defer audioFile.Close()

	mp4File, err := os.OpenFile(*outPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal().Err(err).Str("path", *outPath).Msg("Failed to create output MP4 file")

	}
	defer mp4File.Close()

	// Process video track
	demuxerVideo := mp4.CreateMp4Demuxer(videoFile)
	videoTracksInfo, err := demuxerVideo.ReadHead()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to read video track info")
	}

	log.Debug().
		Interface("tracks_info", videoTracksInfo).
		Msg("Video tracks info")

	mp4VideoInfo := demuxerVideo.GetMp4Info()
	log.Debug().
		Interface("video_info", mp4VideoInfo).
		Msg("MP4 video info")

	demuxerAudio := mp4.CreateMp4Demuxer(audioFile)
	audioTracksInfo, err := demuxerAudio.ReadHead()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to read audio track info")

	}

	log.Debug().
		Interface("tracks_info", audioTracksInfo).
		Msg("Audio tracks info")

	mp4AudioInfo := demuxerAudio.GetMp4Info()
	log.Debug().
		Interface("audio_info", mp4AudioInfo).
		Msg("MP4 audio info")

	muxer, err := mp4.CreateMp4Muxer(mp4File)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create MP4 muxer")

	}

	vtid := muxer.AddVideoTrack(mp4.MP4_CODEC_TYPE(videoTracksInfo[0].Cid))
	atid := muxer.AddAudioTrack(mp4.MP4_CODEC_TYPE(audioTracksInfo[0].Cid))

	log.Info().Msg("Starting video processing...")

	videoStat, _ := videoFile.Stat()
	ratio := uint64(videoStat.Size()) / videoTracksInfo[0].EndDts
	videoBar := progressbar.DefaultBytes(

		int64(videoTracksInfo[0].EndDts*ratio),
		"Processing video",
	)

	for {
		pkg, err := demuxerVideo.ReadPacket()
		if err != nil {
			if err != io.EOF {
				log.Error().Err(err).Msg("Error reading video packet")
			}
			break
		}

		if pkg.Cid == videoTracksInfo[0].Cid {

			if err = muxer.Write(vtid, pkg.Data, uint64(pkg.Pts), uint64(pkg.Dts)); err != nil {
				log.Fatal().Err(err).Msg("Failed to write video packet")

			}

			videoBar.Set64(int64(pkg.Dts * ratio))

		}
	}

	log.Info().Msg("Starting audio processing...")
	audioStat, _ := audioFile.Stat()
	ratio = uint64(audioStat.Size()) / audioTracksInfo[0].EndDts
	audioBar := progressbar.DefaultBytes(
		int64(audioTracksInfo[0].EndDts*ratio),
		"Processing audio",
	)

	for {
		pkg, err := demuxerAudio.ReadPacket()
		if err != nil {
			if err != io.EOF {
				log.Error().Err(err).Msg("Error reading audio packet")
			}
			break
		}

		if pkg.Cid == audioTracksInfo[0].Cid {
			if err = muxer.Write(atid, pkg.Data, uint64(pkg.Pts), uint64(pkg.Dts)); err != nil {
				log.Fatal().Err(err).Msg("Failed to write audio packet")

			}
			audioBar.Set64(int64(pkg.Dts * ratio))
		}
	}

	log.Info().Msg("Writing MP4 trailer...")
	if err = muxer.WriteTrailer(); err != nil {
		log.Fatal().Err(err).Msg("Failed to write MP4 trailer")

	}

	log.Info().Str("output", *outPath).Msg("Processing completed successfully")
}
