# 1) Upscale the video to 1920×1080, keep MP4 output
python client.py \
  --master localhost:50051 \
  --upload ./test_video_large.mp4 \
  --width 320 --height 240 \
  --upscale-width 1920 --upscale-height 1080 \
  --format mp4 \
  --output ./processed_upscaled.mp4

# 2) Downscale the video to 160×120, keep MP4 output
python client.py \
  --master localhost:50051 \
  --upload ./test_video_large.mp4 \
  --width 320 --height 240 \
  --upscale-width 160 --upscale-height 120 \
  --format mp4 \
  --output ./processed_downscaled.mp4

# 3) Convert shards to MKV container (no scaling)
python client.py \
  --master localhost:50051 \
  --upload ./test_video_large.mp4 \
  --width 320 --height 240 \
  --format mkv \
  --output ./processed_video.mkv







# python client.py --master localhost:50062 --upload ./test_video_large.mp4 --width 320 --height 240 --output processed_large_video.mp4