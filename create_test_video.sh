sudo apt update
sudo apt install ffmpeg
ffmpeg -f lavfi -i testsrc=duration=10:size=640x480:rate=24 -f lavfi -i sine=duration=10:frequency=440 -c:v libx264 -c:a aac -strict experimental -shortest test_video.mp4


# Example using ffmpeg to create a dummy video (e.g., 30 seconds long)
ffmpeg -f lavfi -i testsrc=duration=120:size=1920x1080:rate=360 -f lavfi -i sine=duration=600:frequency=1440 -c:v libx264 -c:a aac -strict experimental -shortest test_video_large.mp4
