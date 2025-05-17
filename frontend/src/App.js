// src/App.js
import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

function App() {
  const [videoFile, setVideoFile] = useState(null);
  const [videoId, setVideoId] = useState('');
  const [downloadLink, setDownloadLink] = useState(null);
  const [statusLog, setStatusLog] = useState([]);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [processing, setProcessing] = useState(false);
  const [width, setWidth] = useState(640);
  const [height, setHeight] = useState(480);
  const [upscaleWidth, setUpscaleWidth] = useState(640);
  const [upscaleHeight, setUpscaleHeight] = useState(480);
  const [format, setFormat] = useState('mp4');
  const [showVideoPlayer, setShowVideoPlayer] = useState(false);

  const appendStatus = (message) => {
    setStatusLog((prev) => [...prev, message]);
  };

  const handleUpload = async () => {
    if (!videoFile) return alert('Please select a video file');

    const formData = new FormData();
    formData.append('file', videoFile);
    formData.append('width', width);
    formData.append('height', height);
    formData.append('format', format);

    setUploadProgress(0);
    setProcessing(true);
    appendStatus('⬆ Uploading video...');

    try {
      const res = await axios.post('http://localhost:8000/upload', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        onUploadProgress: (progressEvent) => {
          const percent = Math.round((progressEvent.loaded * 100) / progressEvent.total);
          setUploadProgress(percent);
        }
      });

      if (res.data.success) {
        const id = res.data.video_id;
        setVideoId(id);
        appendStatus(' Uploaded. Waiting for processing...');
        pollStatus(id);
      } else {
        appendStatus(` Upload failed: ${res.data.message}`);
        setProcessing(false);
      }
    } catch (err) {
      console.error(err);
      appendStatus(' Upload failed');
      setProcessing(false);
    }
  };

  const pollStatus = async (id) => {
    const interval = setInterval(async () => {
      try {
        const res = await axios.get(`http://localhost:8000/status/${id}`);
        const { status, message } = res.data;
        appendStatus(` Status: ${status} — ${message}`);

        if (status === 'completed') {
          clearInterval(interval);
          appendStatus('🎞️ Processing complete. Downloading...');
          handleRetrieve(id);
        } else if (status.startsWith('failed') || status === 'not_found') {
          clearInterval(interval);
          appendStatus(`Processing failed: ${message}`);
          setProcessing(false);
        }
      } catch (err) {
        console.error('Polling error:', err);
        clearInterval(interval);
        appendStatus(' Failed to check status.');
        setProcessing(false);
      }
    }, 3000);
  };

  const handleRetrieve = async (id) => {
    try {
      const res = await axios.get(`http://localhost:8000/retrieve/${id}`, {
        responseType: 'blob',
        onDownloadProgress: (progressEvent) => {
          const percent = Math.round((progressEvent.loaded * 100) / progressEvent.total);
          setUploadProgress(percent);
        }
      });
      const url = window.URL.createObjectURL(new Blob([res.data]));
      setDownloadLink(url);
      appendStatus('Download ready');
      setProcessing(false);
      setShowVideoPlayer(true);
    } catch (err) {
      console.error(err);
      appendStatus(' Retrieval failed');
      setProcessing(false);
    }
  };

  return (
    <div className="container">
      <h1>Distributed Video Encoder</h1>
      <div className="card">
        <input type="file" onChange={e => setVideoFile(e.target.files[0])} />

        <div className="param-row">
          <input type="number" placeholder="Width" value={width} onChange={e => setWidth(e.target.value)} />
          <input type="number" placeholder="Height" value={height} onChange={e => setHeight(e.target.value)} />
        </div>

        <div className="param-row">
       
        </div>

        <div className="param-row">
          <select value={format} onChange={e => setFormat(e.target.value)}>
            <option value="mp4">mp4</option>
            <option value="mkv">mkv</option>
            <option value="webm">webm</option>
          </select>
        </div>

        <button onClick={handleUpload} disabled={processing}>
          {processing ? 'Processing...' : 'Upload & Process'}
        </button>

        <div className="progress">
          <div className="bar" style={{ width: `${uploadProgress}%` }}></div>
        </div>

        <ul className="status-log">
          {statusLog.map((msg, idx) => <li key={idx}>{msg}</li>)}
        </ul>

        {videoId && !showVideoPlayer && (
          <button onClick={() => handleRetrieve(videoId)} disabled={processing}>
             Retrieve & Preview
          </button>
        )}

        {showVideoPlayer && (
          <>
            <video
              controls
              style={{ marginTop: '16px', maxWidth: '100%', borderRadius: '8px' }}
            >
              <source src={downloadLink} type="video/mp4" />
              Your browser does not support the video tag.
            </video>
            <a
              href={downloadLink}
              download="processed_video.mp4"
              className="download-link"
            >
              ⬇️ Download Processed Video
            </a>
          </>
        )}
      </div>
    </div>
  );
}

export default App;
