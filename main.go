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

	"github.com/schollz/progressbar/v3"
	log "github.com/sirupsen/logrus"
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

// Vimeo holds our main data, including the final playlist JSON.
type Vimeo struct {
	playlistURL string
	outputPath  string
	response    *PlaylistResponse
	mainBase    string
	concurrency int // user-specified number of threads
}

// -----------------------------------------------------------------------------
// 1) Code to get the final playlistURL from a Vimeo video id:hash if no direct URL
// -----------------------------------------------------------------------------

// fetchVimeoJWT calls https://vimeo.com/_next/jwt to obtain a JWT token.
func fetchVimeoJWT(ctx context.Context) (string, error) {
	log.Debug("Starting fetchVimeoJWT() ...")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://vimeo.com/_next/jwt", nil)
	if err != nil {
		return "", fmt.Errorf("failed to build JWT request: %w", err)
	}
	// Required header
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	log.Debug("Sending request to https://vimeo.com/_next/jwt")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to request JWT: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected HTTP status for JWT: %d", resp.StatusCode)
	}

	// The JSON looks like: {"token":"..."}
	var data struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", fmt.Errorf("failed to parse JWT JSON: %w", err)
	}
	if data.Token == "" {
		return "", errors.New("JWT token not found in response")
	}

	log.Debugf("Successfully fetched JWT: first 15 chars => %s...", data.Token[:15])
	return data.Token, nil
}

// fetchVimeoConfigURL gets the config_url from https://api.vimeo.com/videos/<video_id>?fields=config_url
// If you have a hash, use "video_id:hash" as <video_id>.
func fetchVimeoConfigURL(ctx context.Context, videoID, jwt string) (string, error) {
	apiURL := fmt.Sprintf("https://api.vimeo.com/videos/%s?fields=config_url", videoID)
	log.Debugf("fetchVimeoConfigURL => %s", apiURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to build config_url request: %w", err)
	}
	req.Header.Set("Authorization", "jwt "+jwt)

	log.Debug("Sending request to get config_url ...")
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

	log.Debugf("Received config_url => %s", data.ConfigURL)
	return data.ConfigURL, nil
}

// fetchAVCPlaylistURL calls the config_url to get the "request.files.dash.cdns.<default_cdn>.avc_url"
func fetchAVCPlaylistURL(ctx context.Context, configURL string) (string, error) {
	log.Debugf("fetchAVCPlaylistURL => configURL: %s", configURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, configURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to build configURL request: %w", err)
	}

	log.Debug("Sending request to configURL for AVC ...")
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

	log.Debugf("avc_url => %s", cdn.AVCURL)
	return cdn.AVCURL, nil
}

// resolvePlaylistURLFromVideoID does the entire chain:
//  1. fetch JWT
//  2. fetch config_url
//  3. fetch avc_url
//
// Returns final .json playlist URL or error.
func resolvePlaylistURLFromVideoID(videoID string) (string, error) {
	log.Debugf("resolvePlaylistURLFromVideoID => videoID: %s", videoID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1) JWT
	jwt, err := fetchVimeoJWT(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch Vimeo JWT: %w", err)
	}

	// 2) config_url
	configURL, err := fetchVimeoConfigURL(ctx, videoID, jwt)
	if err != nil {
		return "", fmt.Errorf("failed to fetch config_url: %w", err)
	}

	// 3) avc_url
	avcURL, err := fetchAVCPlaylistURL(ctx, configURL)
	if err != nil {
		return "", fmt.Errorf("failed to fetch avc_url: %w", err)
	}

	return avcURL, nil
}

// -----------------------------------------------------------------------------
// 2) Fetch and parse the final playlist JSON
// -----------------------------------------------------------------------------

func (v *Vimeo) SendRequest() error {
	log.Debugf("SendRequest => fetching playlist from: %s", v.playlistURL)

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

	// Decode JSON directly from the response body
	var pr PlaylistResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return fmt.Errorf("failed to decode playlist JSON: %w", err)
	}

	v.response = &pr
	log.Infof("Playlist JSON successfully retrieved. ClipID: %s, BaseURL: %s", pr.ClipID, pr.BaseURL)
	return nil
}

// ParsePlaylist sets up v.mainBase and sorts video/audio streams to easily find best streams.
func (v *Vimeo) ParsePlaylist() error {
	if v.response == nil {
		return errors.New("no response data available")
	}

	log.Debug("Sorting video by descending bitrate ...")
	// Sort video streams by (bitrate or avg_bitrate) descending
	sort.Slice(v.response.Video, func(i, j int) bool {
		return v.response.Video[i].Bitrate > v.response.Video[j].Bitrate
	})

	log.Debug("Sorting audio by descending bitrate ...")
	// Sort audio streams by bitrate descending
	sort.Slice(v.response.Audio, func(i, j int) bool {
		return v.response.Audio[i].Bitrate > v.response.Audio[j].Bitrate
	})

	baseURL, err := url.Parse(v.playlistURL)
	if err != nil {
		return fmt.Errorf("failed to parse playlistURL: %w", err)
	}
	ref, err := url.Parse(v.response.BaseURL)
	if err != nil {
		return fmt.Errorf("failed to parse response.BaseURL: %w", err)
	}
	resolved := baseURL.ResolveReference(ref).String()
	v.mainBase = resolved

	log.Debugf("Set v.mainBase => %s", v.mainBase)
	return nil
}

// BestVideoStream returns the first (best) video stream after sorting.
func (v *Vimeo) BestVideoStream() *VideoStream {
	if len(v.response.Video) == 0 {
		return nil
	}
	best := &v.response.Video[0]
	log.Debugf("Best video => ID:%s, resolution:%dx%d, bitrate:%d",
		best.ID, best.Width, best.Height, best.Bitrate)
	return best
}

// BestAudioStream returns the first (best) audio stream after sorting.
func (v *Vimeo) BestAudioStream() *AudioStream {
	if len(v.response.Audio) == 0 {
		return nil
	}
	best := &v.response.Audio[0]
	log.Debugf("Best audio => ID:%s, channels:%d, bitrate:%d",
		best.ID, best.Channels, best.Bitrate)
	return best
}

// buildSegmentURL resolves the segment URL relative to the base.
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

// -----------------------------------------------------------------------------
// 4) Concurrency: download segments, store them in memory, then write in order
// -----------------------------------------------------------------------------

// downloadSegments concurrently downloads all segments in the correct order:
//  1. decode and write initSegment
//  2. download all segments in parallel (or sequential if concurrency=1)
//  3. write out segments in ascending index
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

	// 1) Write the init segment
	initData, err := base64.StdEncoding.DecodeString(initBase64)
	if err != nil {
		return fmt.Errorf("failed to decode init segment: %w", err)
	}
	log.Debugf("Writing init segment of length %d to => %s", len(initData), outputFileName)

	if _, err := f.Write(initData); err != nil {
		return fmt.Errorf("failed to write init segment: %w", err)
	}
	log.Infof("[downloadSegments] Wrote init segment to %s", outputFileName)

	// 2) Download all segments in parallel (limit concurrency to v.concurrency)
	concurrency := v.concurrency
	if concurrency < 1 {
		concurrency = 1
	}
	log.Debugf("Starting concurrent downloads with concurrency=%d for %s", concurrency, outputFileName)

	results := make([][]byte, len(segmentList)) // each entry is the downloaded bytes for segment i
	errorsChan := make(chan error, len(segmentList))

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency) // concurrency limiter

	for idx, seg := range segmentList {
		wg.Add(1)
		go func(i int, s Segment) {
			defer wg.Done()
			sem <- struct{}{} // acquire concurrency slot
			defer func() { <-sem }()

			segURL, errBuild := buildSegmentURL(v.mainBase, s.URL)
			if errBuild != nil {
				errorsChan <- fmt.Errorf("failed to build segment URL for idx %d: %w", i, errBuild)
				return
			}

			log.Debugf("Downloading segment idx=%d => %s", i, segURL)
			data, errDl := downloadSegment(segURL)
			if errDl != nil {
				errorsChan <- fmt.Errorf("failed to download segment idx %d: %w", i, errDl)
				return
			}
			results[i] = data
			log.Debugf("Segment idx=%d downloaded, size=%d bytes", i, len(data))
		}(idx, seg)
	}

	wg.Wait()
	close(errorsChan)

	// if anything went wrong, return the first error
	for e := range errorsChan {
		if e != nil {
			return e
		}
	}

	// 3) Write out segments in ascending order
	for i, segData := range results {
		log.Debugf("Writing segment idx=%d to file => length=%d bytes", i, len(segData))
		if _, err := f.Write(segData); err != nil {
			return fmt.Errorf("failed writing segment idx %d to file: %w", i, err)
		}
	}

	log.Infof("[downloadSegments] Downloaded and concatenated %d segments into %s", len(segmentList), outputFileName)
	return nil
}

// downloadSegment performs an HTTP GET, returning the entire response body.
func downloadSegment(segURL string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, segURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	log.Debugf("downloadSegment => GET %s", segURL)

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

// -----------------------------------------------------------------------------
// main()
// -----------------------------------------------------------------------------

func main() {
	// Configure the log output formatter
	log.SetFormatter(&log.TextFormatter{
		// Set timestamp format to include milliseconds
		TimestampFormat: "2006-01-02 15:04:05.000",
		// Force colored output even when not running in a terminal
		ForceColors: true,
		// Show full timestamp in each log entry
		FullTimestamp: true,
		// Do not truncate the log level text
		DisableLevelTruncation: true,
		// Pad level text for neat columns
		PadLevelText: true,
	})

	log.SetLevel(log.DebugLevel)

	videoIDFlag := flag.String("video-id", "", "Vimeo video ID (optionally with :hash or /hash) e.g. 12345:abcd")
	playlistURLFlag := flag.String("url", "", "Direct playlist JSON URL (optional)")
	outputFlag := flag.String("output", ".", "Output directory")
	threadsFlag := flag.Int("threads", 6, "Number of threads for segment download (1 = sequential)")
	videoPath := flag.String("video", "best_video.mp4", "Path to best_video.mp4")
	audioPath := flag.String("audio", "best_audio.m4a", "Path to best_audio.m4a")
	outPath := flag.String("out", "result.mp4", "Path for the merged output file")
	flag.Parse()

	if *videoIDFlag == "" && *playlistURLFlag == "" {
		log.Fatal("Either --video <id> or --url <playlistURL> must be provided")
	}

	// If user gave a direct playlist URL, use it; otherwise auto-resolve from video ID
	var finalPlaylistURL string
	if *playlistURLFlag != "" {
		finalPlaylistURL = *playlistURLFlag
		log.Infof("Using direct playlist URL: %s", finalPlaylistURL)
	} else {
		// e.g. "12345:abcd" or "12345/abcd" or just "12345"
		raw := *videoIDFlag
		log.Debugf("User-provided video ID or ID+hash => %s", raw)

		var forAPI string
		if strings.Contains(raw, ":") {
			// user typed 12345:abcd => good
			forAPI = raw
		} else if strings.Contains(raw, "/") {
			// user typed 12345/abcd => we want "12345:abcd"
			parts := strings.SplitN(raw, "/", 2)
			forAPI = parts[0] + ":" + parts[1]
		} else {
			// no hash
			forAPI = raw
		}
		log.Debugf("For Vimeo API => %s", forAPI)

		resolved, err := resolvePlaylistURLFromVideoID(forAPI)
		if err != nil {
			log.Fatalf("Could not resolve playlist URL from video ID '%s': %v", raw, err)
		}
		finalPlaylistURL = resolved
		log.Infof("Resolved final playlist URL => %s", finalPlaylistURL)
	}

	// Initialize Vimeo struct
	vimeo := &Vimeo{
		playlistURL: finalPlaylistURL,
		outputPath:  *outputFlag,
		concurrency: *threadsFlag,
	}

	// 1) Fetch and parse the playlist
	if err := vimeo.SendRequest(); err != nil {
		log.Fatalf("Unable to get playlist JSON: %v", err)
	}
	if err := vimeo.ParsePlaylist(); err != nil {
		log.Fatalf("Unable to parse playlist: %v", err)
	}

	// 2) Select best video + audio streams
	bestVideo := vimeo.BestVideoStream()
	bestAudio := vimeo.BestAudioStream()

	if bestVideo == nil && bestAudio == nil {
		log.Fatal("No video or audio streams found in the playlist JSON")
	}

	// 3) Download the best streams
	if bestVideo != nil {
		fileVideo := *videoPath
		log.Infof("Downloading best video => %s", fileVideo)
		err := vimeo.downloadSegments(bestVideo.Segments, bestVideo.InitSegment, fileVideo)
		if err != nil {
			log.Fatalf("Failed to download best video: %v", err)
		}
	}
	if bestAudio != nil {
		fileAudio := *audioPath
		log.Infof("Downloading best audio => %s", fileAudio)
		err := vimeo.downloadSegments(bestAudio.Segments, bestAudio.InitSegment, fileAudio)
		if err != nil {
			log.Fatalf("Failed to download best audio: %v", err)
		}
	}

	videoFile, err := os.Open(*videoPath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer videoFile.Close()

	audioFile, err := os.Open(*audioPath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer audioFile.Close()

	mp4File, err := os.OpenFile(*outPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer mp4File.Close()

	demuxerVideo := mp4.CreateMp4Demuxer(videoFile)
	videoTracksInfo, err := demuxerVideo.ReadHead()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("videoTracksInfo: %+v\n", videoTracksInfo)
	mp4VideoInfo := demuxerVideo.GetMp4Info()
	fmt.Printf("mp4VideoInfo: %+v\n", mp4VideoInfo)

	demuxerAudio := mp4.CreateMp4Demuxer(audioFile)
	audioTracksInfo, err := demuxerAudio.ReadHead()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("audioTracksInfo: %+v\n", audioTracksInfo)
	mp4AudioInfo := demuxerVideo.GetMp4Info()
	fmt.Printf("mp4AudioInfo: %+v\n", mp4AudioInfo)

	fmt.Println(mp4File.Seek(0, io.SeekCurrent))
	muxer, err := mp4.CreateMp4Muxer(mp4File)
	if err != nil {
		fmt.Println(err)
		return
	}

	vtid := muxer.AddVideoTrack(mp4.MP4_CODEC_TYPE(videoTracksInfo[0].Cid))
	atid := muxer.AddAudioTrack(mp4.MP4_CODEC_TYPE(audioTracksInfo[0].Cid))

	fmt.Printf("int64(videoTracksInfo[0].EndDts): %v\n", int64(videoTracksInfo[0].EndDts))
	// Get video file size for progress bar
	videoStat, _ := videoFile.Stat()
	ratio := uint64(videoStat.Size()) / videoTracksInfo[0].EndDts
	videoBar := progressbar.DefaultBytes(
		//videoStat.Size(),
		int64(videoTracksInfo[0].EndDts*ratio),
		"Processing video",
	)

	//progressbar.Default()
	for {
		pkg, err := demuxerVideo.ReadPacket()
		if err != nil {
			//fmt.Println(err)
			break
		}
		//percentComplete := (100 * pkg.Dts) / videoTracksInfo[0].EndDts

		if pkg.Cid == videoTracksInfo[0].Cid {
			//fmt.Printf("track:%d,cid:%+v,pts:%d dts:%d\n", pkg.TrackId, pkg.Cid, pkg.Pts, pkg.Dts)
			//fmt.Printf("pts:%d dts:%d endDts:%d\n", pkg.Pts, pkg.Dts, videoTracksInfo[0].EndDts)
			err = muxer.Write(vtid, pkg.Data, uint64(pkg.Pts), uint64(pkg.Dts))
			if err != nil {
				panic(err)
			}
			//videoBar.Add(int(pkg.Dts - pkg.Pts))
			videoBar.Set64(int64(pkg.Dts * ratio))

		}
	}

	// Get audio file size for progress bar
	audioStat, _ := audioFile.Stat()
	ratio = uint64(audioStat.Size()) / audioTracksInfo[0].EndDts
	audioBar := progressbar.DefaultBytes(
		int64(audioTracksInfo[0].EndDts*ratio),
		"Processing audio",
	)

	for {
		pkg, err := demuxerAudio.ReadPacket()
		if err != nil {
			break
		}
		if pkg.Cid == audioTracksInfo[0].Cid {
			err = muxer.Write(atid, pkg.Data, uint64(pkg.Pts), uint64(pkg.Dts))
			if err != nil {
				panic(err)
			}
			audioBar.Set64(int64(pkg.Dts * ratio))
		}
	}

	fmt.Println("write trailer")
	err = muxer.WriteTrailer()
	if err != nil {
		panic(err)
	}

	log.Info("All done. The result.mp4 is in your output folder.")
}
