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
	"regexp"
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

func fetchVimeoJWT(ctx context.Context) (string, error) {
	log.Trace().Msg("Entering fetchVimeoJWT()")
	defer log.Trace().Msg("Exiting fetchVimeoJWT()")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://vimeo.com/_next/jwt", nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create JWT request")
		return "", fmt.Errorf("failed to build JWT request: %w", err)
	}

	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	log.Debug().Interface("headers", req.Header).Msg("Request headers set")

	log.Debug().Msg("Sending request to https://vimeo.com/_next/jwt")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error().Err(err).Msg("Failed to request JWT")
		return "", fmt.Errorf("failed to request JWT: %w", err)
	}
	defer resp.Body.Close()

	log.Debug().Int("status_code", resp.StatusCode).Msg("Received response")
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

	log.Debug().Str("token_prefix", data.Token[:15]+"...").Int("token_length", len(data.Token)).Msg("JWT token decoded")
	if data.Token == "" {
		log.Error().Msg("JWT token not found in response")
		return "", errors.New("JWT token not found in response")
	}

	return data.Token, nil
}

func fetchVimeoConfigURL(ctx context.Context, videoID, jwt string) (string, error) {
	log.Trace().Str("videoID", videoID).Msg("Entering fetchVimeoConfigURL()")
	defer log.Trace().Msg("Exiting fetchVimeoConfigURL()")

	apiURL := fmt.Sprintf("https://api.vimeo.com/videos/%s?fields=config_url", videoID)
	log.Debug().Str("url", apiURL).Str("jwt_prefix", jwt[:15]+"...").Msg("Preparing config URL request")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		log.Error().Err(err).Str("url", apiURL).Msg("Failed to create config URL request")
		return "", fmt.Errorf("failed to build config_url request: %w", err)
	}
	req.Header.Set("Authorization", "jwt "+jwt)
	log.Debug().Interface("headers", req.Header).Msg("Request headers set")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error().Err(err).Str("url", apiURL).Msg("Failed to execute config URL request")
		return "", fmt.Errorf("failed to request config_url: %w", err)
	}
	defer resp.Body.Close()

	log.Debug().Int("status_code", resp.StatusCode).Msg("Received response")
	if resp.StatusCode != http.StatusOK {
		log.Error().Int("status_code", resp.StatusCode).Msg("Unexpected status code for config URL")
		return "", fmt.Errorf("unexpected HTTP status for config_url: %d", resp.StatusCode)
	}

	var data struct {
		ConfigURL string `json:"config_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		log.Error().Err(err).Msg("Failed to decode config URL response")
		return "", fmt.Errorf("failed to parse config_url JSON: %w", err)
	}

	log.Debug().Str("config_url", data.ConfigURL).Msg("Config URL successfully retrieved")
	if data.ConfigURL == "" {
		log.Error().Msg("Empty config URL in response")
		return "", errors.New("config_url not found in response")
	}

	return data.ConfigURL, nil
}

func fetchAVCPlaylistURL(ctx context.Context, configURL string) (string, string, error) {
	log.Trace().Str("configURL", configURL).Msg("Entering fetchAVCPlaylistURL()")
	defer log.Trace().Msg("Exiting fetchAVCPlaylistURL()")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, configURL, nil)
	if err != nil {
		log.Error().Err(err).Str("url", configURL).Msg("Failed to create AVC playlist request")
		return "", "", fmt.Errorf("failed to build configURL request: %w", err)
	}

	log.Debug().Interface("headers", req.Header).Msg("Request headers set")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error().Err(err).Str("url", configURL).Msg("Failed to execute AVC playlist request")
		return "", "", fmt.Errorf("failed to request configURL: %w", err)
	}
	defer resp.Body.Close()

	log.Debug().Int("status_code", resp.StatusCode).Msg("Received response")
	if resp.StatusCode != http.StatusOK {
		log.Error().Int("status_code", resp.StatusCode).Msg("Unexpected status code for AVC playlist")
		return "", "", fmt.Errorf("unexpected HTTP status from config_url: %d", resp.StatusCode)
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
		Video struct {
			Title string `json:"title"`
		} `json:"video"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		log.Error().Err(err).Msg("Failed to decode AVC playlist response")
		return "", "", fmt.Errorf("failed to parse config JSON: %w", err)
	}

	defaultCDN := data.Request.Files.Dash.DefaultCDN
	log.Debug().Str("default_cdn", defaultCDN).Msg("Default CDN identified")

	if defaultCDN == "" {
		log.Error().Msg("Default CDN not found in config")
		return "", "", errors.New("default_cdn not found in config")
	}

	cdn, ok := data.Request.Files.Dash.CDNs[defaultCDN]
	if !ok {
		log.Error().Str("cdn", defaultCDN).Msg("CDN not found in dash.cdns")
		return "", "", fmt.Errorf("CDN '%s' not found in dash.cdns", defaultCDN)
	}

	if cdn.AVCURL == "" {
		log.Error().Msg("AVC URL not found in config")
		return "", "", errors.New("avc_url not found in config")
	}

	// Clean the title for use as filename
	title := sanitizeFilename(data.Video.Title)
	if title == "" {
		title = "vimeo_video" // fallback name
	}

	log.Debug().
		Str("avc_url", cdn.AVCURL).
		Str("title", title).
		Msg("AVC URL and title successfully retrieved")
	return cdn.AVCURL, title, nil
}

// sanitizeFilename removes or replaces characters that are invalid in filenames
func sanitizeFilename(name string) string {
	// Replace invalid characters with underscores
	invalid := regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
	name = invalid.ReplaceAllString(name, "_")

	// Trim spaces from ends
	name = strings.TrimSpace(name)

	// Replace multiple spaces/underscores with single underscore
	name = regexp.MustCompile(`[\s_]+`).ReplaceAllString(name, "_")

	return name
}

func resolvePlaylistURLFromVideoID(videoID string) (string, string, error) {
	log.Trace().Str("videoID", videoID).Msg("Entering resolvePlaylistURLFromVideoID()")
	defer log.Trace().Msg("Exiting resolvePlaylistURLFromVideoID()")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Debug().Msg("Fetching Vimeo JWT...")
	jwt, err := fetchVimeoJWT(ctx)
	if err != nil {
		return "", "", fmt.Errorf("failed to fetch Vimeo JWT: %w", err)
	}

	log.Debug().Msg("Fetching config URL...")
	configURL, err := fetchVimeoConfigURL(ctx, videoID, jwt)
	if err != nil {
		return "", "", fmt.Errorf("failed to fetch config_url: %w", err)
	}

	log.Debug().Msg("Fetching AVC URL...")
	avcURL, title, err := fetchAVCPlaylistURL(ctx, configURL)
	if err != nil {
		return "", "", fmt.Errorf("failed to fetch avc_url: %w", err)
	}

	log.Debug().
		Str("avc_url", avcURL).
		Str("title", title).
		Msg("Successfully resolved playlist URL and title")
	return avcURL, title, nil
}

func (v *Vimeo) SendRequest() error {
	log.Trace().Str("url", v.playlistURL).Msg("Entering SendRequest()")
	defer log.Trace().Msg("Exiting SendRequest()")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.playlistURL, nil)
	if err != nil {
		log.Error().Err(err).Str("url", v.playlistURL).Msg("Failed to create playlist request")
		return fmt.Errorf("failed to create request for playlistURL: %w", err)
	}

	log.Debug().Interface("headers", req.Header).Msg("Request headers set")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error().Err(err).Str("url", v.playlistURL).Msg("Failed to execute playlist request")
		return fmt.Errorf("failed to GET playlist URL: %w", err)
	}
	defer resp.Body.Close()

	log.Debug().Int("status_code", resp.StatusCode).Msg("Received response")
	if resp.StatusCode != http.StatusOK {
		log.Error().Int("status_code", resp.StatusCode).Msg("Unexpected status code for playlist")
		return fmt.Errorf("unexpected status code: %d for playlist JSON", resp.StatusCode)
	}

	var pr PlaylistResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		log.Error().Err(err).Msg("Failed to decode playlist response")
		return fmt.Errorf("failed to decode playlist JSON: %w", err)
	}

	v.response = &pr
	log.Info().
		Str("clipID", pr.ClipID).
		Str("baseURL", pr.BaseURL).
		Int("video_streams", len(pr.Video)).
		Int("audio_streams", len(pr.Audio)).
		Msg("Playlist JSON successfully retrieved")
	return nil
}

func (v *Vimeo) ParsePlaylist() error {
	log.Trace().Msg("Entering ParsePlaylist()")
	defer log.Trace().Msg("Exiting ParsePlaylist()")

	if v.response == nil {
		log.Error().Msg("No response data available")
		return errors.New("no response data available")
	}

	log.Debug().Int("streams", len(v.response.Video)).Msg("Sorting video streams by descending bitrate")

	sort.Slice(v.response.Video, func(i, j int) bool {
		return v.response.Video[i].Bitrate > v.response.Video[j].Bitrate
	})

	log.Debug().Int("streams", len(v.response.Audio)).Msg("Sorting audio streams by descending bitrate")

	sort.Slice(v.response.Audio, func(i, j int) bool {
		return v.response.Audio[i].Bitrate > v.response.Audio[j].Bitrate
	})

	baseURL, err := url.Parse(v.playlistURL)
	if err != nil {
		log.Error().Err(err).Str("url", v.playlistURL).Msg("Failed to parse playlist URL")
		return fmt.Errorf("failed to parse playlistURL: %w", err)
	}

	ref, err := url.Parse(v.response.BaseURL)
	if err != nil {
		log.Error().Err(err).Str("url", v.response.BaseURL).Msg("Failed to parse response base URL")
		return fmt.Errorf("failed to parse response.BaseURL: %w", err)
	}

	resolved := baseURL.ResolveReference(ref).String()
	v.mainBase = resolved

	log.Debug().
		Str("playlist_url", v.playlistURL).
		Str("base_url", v.response.BaseURL).
		Str("main_base", resolved).
		Msg("Base URLs resolved")

	return nil
}

func (v *Vimeo) BestVideoStream() *VideoStream {
	log.Trace().Msg("Entering BestVideoStream()")
	defer log.Trace().Msg("Exiting BestVideoStream()")

	if len(v.response.Video) == 0 {
		log.Debug().Msg("No video streams available")
		return nil
	}

	best := &v.response.Video[0]
	log.Debug().
		Str("id", best.ID).
		Int("width", best.Width).
		Int("height", best.Height).
		Int("bitrate", best.Bitrate).
		Int("avg_bitrate", best.AvgBitrate).
		Str("codecs", best.Codecs).
		Msg("Best video stream selected")
	return best
}

func (v *Vimeo) BestAudioStream() *AudioStream {
	log.Trace().Msg("Entering BestAudioStream()")
	defer log.Trace().Msg("Exiting BestAudioStream()")

	if len(v.response.Audio) == 0 {
		log.Debug().Msg("No audio streams available")
		return nil
	}

	best := &v.response.Audio[0]
	log.Debug().
		Str("id", best.ID).
		Int("channels", best.Channels).
		Int("bitrate", best.Bitrate).
		Int("sample_rate", best.SampleRate).
		Msg("Best audio stream selected")
	return best
}

func buildSegmentURL(base, segURL string) (string, error) {
	log.Trace().
		Str("base", base).
		Str("segment_url", segURL).
		Msg("Entering buildSegmentURL()")
	defer log.Trace().Msg("Exiting buildSegmentURL()")

	segBase, err := url.Parse(base)
	if err != nil {
		log.Error().Err(err).Str("base", base).Msg("Failed to parse base URL")
		return "", fmt.Errorf("failed to parse base URL: %w", err)
	}

	ref, err := url.Parse(segURL)
	if err != nil {
		log.Error().Err(err).Str("segment_url", segURL).Msg("Failed to parse segment URL")
		return "", fmt.Errorf("failed to parse segment URL: %w", err)
	}

	resolved := segBase.ResolveReference(ref).String()
	log.Debug().
		Str("base", base).
		Str("segment", segURL).
		Str("resolved", resolved).
		Msg("Segment URL resolved")

	return resolved, nil
}

func (v *Vimeo) downloadSegments(segmentList []Segment, initBase64 string, outputFileName string) error {
	log.Trace().
		Int("segments", len(segmentList)).
		Str("output", outputFileName).
		Msg("Entering downloadSegments()")
	defer log.Trace().Msg("Exiting downloadSegments()")

	if len(segmentList) == 0 {
		log.Error().Str("output", outputFileName).Msg("Empty segment list")
		return fmt.Errorf("segmentList is empty, cannot download => %s", outputFileName)
	}

	outPath := filepath.Join(v.outputPath, outputFileName)
	log.Debug().Str("path", outPath).Msg("Creating output file")

	f, err := os.Create(outPath)
	if err != nil {
		log.Error().Err(err).Str("path", outPath).Msg("Failed to create output file")
		return fmt.Errorf("failed to create output file %s: %w", outPath, err)
	}
	defer f.Close()

	initData, err := base64.StdEncoding.DecodeString(initBase64)
	if err != nil {
		log.Error().Err(err).Msg("Failed to decode init segment")
		return fmt.Errorf("failed to decode init segment: %w", err)
	}

	log.Debug().
		Int("length", len(initData)).
		Str("file", outputFileName).
		Msg("Writing init segment")

	if _, err := f.Write(initData); err != nil {
		log.Error().Err(err).Msg("Failed to write init segment")
		return fmt.Errorf("failed to write init segment: %w", err)
	}

	concurrency := v.concurrency
	if concurrency < 1 {
		concurrency = 1
	}
	log.Debug().
		Int("concurrency", concurrency).
		Str("file", outputFileName).
		Int("segments", len(segmentList)).
		Msg("Starting concurrent downloads")

	results := make([][]byte, len(segmentList))
	errorsChan := make(chan error, len(segmentList))

	// Create progress bar for downloads
	bar := progressbar.Default(
		int64(len(segmentList)),
		fmt.Sprintf("Downloading %s", outputFileName),
	)

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	progressChan := make(chan struct{}, len(segmentList))

	// Start progress monitoring goroutine
	go func() {
		for range progressChan {
			bar.Add(1)
		}
	}()

	for idx, seg := range segmentList {
		wg.Add(1)
		go func(i int, s Segment) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			log.Trace().
				Int("index", i).
				Float64("start", s.Start).
				Float64("end", s.End).
				Msg("Processing segment")

			segURL, errBuild := buildSegmentURL(v.mainBase, s.URL)
			if errBuild != nil {
				log.Error().Err(errBuild).Int("index", i).Msg("Failed to build segment URL")
				errorsChan <- fmt.Errorf("failed to build segment URL for idx %d: %w", i, errBuild)
				return
			}

			data, errDl := downloadSegment(segURL)
			if errDl != nil {
				log.Error().Err(errDl).Int("index", i).Str("url", segURL).Msg("Failed to download segment")
				errorsChan <- fmt.Errorf("failed to download segment idx %d: %w", i, errDl)
				return
			}

			results[i] = data
			progressChan <- struct{}{} // Signal progress

			log.Debug().
				Int("index", i).
				Int("size", len(data)).
				Str("url", segURL).
				Msg("Segment downloaded successfully")
		}(idx, seg)
	}

	wg.Wait()
	close(progressChan)
	close(errorsChan)

	bar.Finish()

	for e := range errorsChan {
		if e != nil {
			log.Error().Err(e).Msg("Error encountered during segment downloads")
			return e
		}
	}

	// Create progress bar for writing segments to file
	writeBar := progressbar.DefaultBytes(
		getTotalSize(results),
		"Writing segments to file",
	)

	for i, segData := range results {
		log.Trace().
			Int("index", i).
			Int("size", len(segData)).
			Msg("Writing segment to file")

		if _, err := f.Write(segData); err != nil {
			log.Error().Err(err).Int("index", i).Msg("Failed writing segment to file")
			return fmt.Errorf("failed writing segment idx %d to file: %w", i, err)
		}
		writeBar.Add(len(segData))
	}

	log.Info().
		Int("segments", len(segmentList)).
		Str("file", outputFileName).
		Int64("total_size", getTotalSize(results)).
		Msg("Downloaded and concatenated segments successfully")
	return nil
}

func getTotalSize(data [][]byte) int64 {
	var total int64
	for _, d := range data {
		total += int64(len(d))
	}
	return total
}

func downloadSegment(segURL string) ([]byte, error) {
	log.Trace().Str("url", segURL).Msg("Entering downloadSegment()")
	defer log.Trace().Msg("Exiting downloadSegment()")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, segURL, nil)
	if err != nil {
		log.Error().Err(err).Str("url", segURL).Msg("Failed to create segment request")
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error().Err(err).Str("url", segURL).Msg("Failed to download segment")
		return nil, fmt.Errorf("failed to GET segment: %w", err)
	}
	defer resp.Body.Close()

	log.Debug().
		Int("status_code", resp.StatusCode).
		Str("url", segURL).
		Msg("Received segment response")

	if resp.StatusCode != http.StatusOK {
		log.Error().
			Int("status_code", resp.StatusCode).
			Str("url", segURL).
			Msg("Unexpected status code for segment")
		return nil, fmt.Errorf("segment GET returned status %d for URL: %s", resp.StatusCode, segURL)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error().Err(err).Str("url", segURL).Msg("Failed to read segment body")
		return nil, fmt.Errorf("failed to read segment body: %w", err)
	}

	log.Debug().
		Int("size", len(data)).
		Str("url", segURL).
		Msg("Segment downloaded successfully")

	return data, nil
}

func main() {
	// Configure zerolog
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
	zerolog.SetGlobalLevel(zerolog.InfoLevel) // Changed from DebugLevel to TraceLevel
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "2006-01-02 15:04:05.000",
		NoColor:    false,
	}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()

	log.Trace().Msg("Starting application...")

	videoIDFlag := flag.String("video-id", "", "Vimeo video ID (optionally with :hash or /hash) e.g. 12345:abcd")
	playlistURLFlag := flag.String("url", "", "Direct playlist JSON URL (optional)")
	outputFlag := flag.String("output", ".", "Output directory")
	threadsFlag := flag.Int("threads", 6, "Number of threads for segment download (1 = sequential)")
	videoPath := flag.String("video", "best_video.mp4", "Path to best_video.mp4")
	audioPath := flag.String("audio", "best_audio.m4a", "Path to best_audio.m4a")
	outPath := flag.String("out", "result.mp4", "Path for the merged output file")

	flag.Parse()

	log.Debug().
		Str("video_id", *videoIDFlag).
		Str("playlist_url", *playlistURLFlag).
		Str("output_dir", *outputFlag).
		Int("threads", *threadsFlag).
		Str("video_path", *videoPath).
		Str("audio_path", *audioPath).
		Str("out_path", *outPath).
		Msg("Parsed command line flags")

	if *videoIDFlag == "" && *playlistURLFlag == "" {
		log.Fatal().Msg("Either --video <id> or --url <playlistURL> must be provided")
	}

	var finalPlaylistURL, videoTitle string
	if *playlistURLFlag != "" {
		finalPlaylistURL = *playlistURLFlag
		videoTitle = "vimeo_video" // Default title for direct URL mode
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

		log.Debug().
			Str("raw_id", raw).
			Str("api_id", forAPI).
			Msg("Formatted ID for API")

		resolved, title, err := resolvePlaylistURLFromVideoID(forAPI)
		if err != nil {
			log.Fatal().Err(err).Str("video_id", raw).Msg("Could not resolve playlist URL")
		}
		finalPlaylistURL = resolved
		videoTitle = title
		log.Info().
			Str("url", finalPlaylistURL).
			Str("title", videoTitle).
			Msg("Resolved final playlist URL and title")
	}
	if *videoPath == "best_video.mp4" {
		*videoPath = videoTitle + "_video.mp4"
	}
	if *audioPath == "best_audio.m4a" {
		*audioPath = videoTitle + "_audio.m4a"
	}
	if *outPath == "result.mp4" {
		*outPath = videoTitle + ".mp4"
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
	log.Debug().Msg("Opening video and audio files for processing")

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

	log.Debug().Msg("Creating MP4 demuxers")

	// Process video track
	demuxerVideo := mp4.CreateMp4Demuxer(videoFile)
	videoTracksInfo, err := demuxerVideo.ReadHead()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to read video track info")
	}

	log.Debug().
		Interface("tracks_info", videoTracksInfo).
		Msg("Video tracks info retrieved")

	mp4VideoInfo := demuxerVideo.GetMp4Info()
	log.Debug().
		Interface("video_info", mp4VideoInfo).
		Msg("MP4 video info retrieved")

	demuxerAudio := mp4.CreateMp4Demuxer(audioFile)
	audioTracksInfo, err := demuxerAudio.ReadHead()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to read audio track info")

	}

	log.Debug().
		Interface("tracks_info", audioTracksInfo).
		Msg("Audio tracks info retrieved")

	mp4AudioInfo := demuxerAudio.GetMp4Info()
	log.Debug().
		Interface("audio_info", mp4AudioInfo).
		Msg("MP4 audio info retrieved")

	log.Debug().Msg("Creating MP4 muxer")
	muxer, err := mp4.CreateMp4Muxer(mp4File)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create MP4 muxer")

	}

	log.Debug().Msg("Adding video and audio tracks to muxer")
	vtid := muxer.AddVideoTrack(mp4.MP4_CODEC_TYPE(videoTracksInfo[0].Cid))
	atid := muxer.AddAudioTrack(mp4.MP4_CODEC_TYPE(audioTracksInfo[0].Cid))

	log.Info().Msg("Starting video processing...")

	videoStat, err := videoFile.Stat()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get video file stats")
	}

	ratio := uint64(videoStat.Size()) / videoTracksInfo[0].EndDts
	videoBar := progressbar.DefaultBytes(

		int64(videoTracksInfo[0].EndDts*ratio),
		"Processing video",
	)

	log.Debug().Msg("Starting video packet processing loop")
	for {
		pkg, err := demuxerVideo.ReadPacket()
		if err != nil {
			if err != io.EOF {
				log.Error().Err(err).Msg("Error reading video packet")
			} else {
				log.Debug().Msg("Reached end of video file")
			}
			break
		}

		if pkg.Cid == videoTracksInfo[0].Cid {
			log.Trace().
				Uint64("pts", pkg.Pts).
				Uint64("dts", pkg.Dts).
				Int("size", len(pkg.Data)).
				Msg("Processing video packet")

			if err = muxer.Write(vtid, pkg.Data, uint64(pkg.Pts), uint64(pkg.Dts)); err != nil {
				log.Fatal().Err(err).Msg("Failed to write video packet")

			}

			videoBar.Set64(int64(pkg.Dts * ratio))

		}
	}

	log.Info().Msg("Starting audio processing...")

	audioStat, err := audioFile.Stat()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get audio file stats")
	}

	ratio = uint64(audioStat.Size()) / audioTracksInfo[0].EndDts
	audioBar := progressbar.DefaultBytes(
		int64(audioTracksInfo[0].EndDts*ratio),
		"Processing audio",
	)

	log.Debug().Msg("Starting audio packet processing loop")
	for {
		pkg, err := demuxerAudio.ReadPacket()
		if err != nil {
			if err != io.EOF {
				log.Error().Err(err).Msg("Error reading audio packet")
			} else {
				log.Debug().Msg("Reached end of audio file")
			}
			break
		}

		if pkg.Cid == audioTracksInfo[0].Cid {
			log.Trace().
				Uint64("pts", pkg.Pts).
				Uint64("dts", pkg.Dts).
				Int("size", len(pkg.Data)).
				Msg("Processing audio packet")

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

	log.Info().
		Str("output", *outPath).
		Msg("Processing completed successfully")
}
