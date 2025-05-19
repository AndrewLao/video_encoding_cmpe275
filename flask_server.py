from flask_cors import CORS
from flask import Flask, request, jsonify, Response
import grpc
import replication_pb2
import replication_pb2_grpc
import os

app = Flask(__name__)
CORS(app)

# gRPC target (master node)
GRPC_MASTER_ADDRESS = 'localhost:50053'
CHUNK_SIZE = 1024 * 1024  # 1MB


def generate_chunks(file_path, video_id, width, height, upscale_width, upscale_height, format):
    first = True
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break

            if first:
                yield replication_pb2.UploadVideoChunk(
                    video_id=video_id,
                    data_chunk=chunk,
                    target_width=width,
                    target_height=height,
                    upscale_width=upscale_width,
                    upscale_height=upscale_height,
                    output_format=format,
                    original_filename=os.path.basename(file_path),
                    is_first_chunk=True
                )
                first = False
            else:
                yield replication_pb2.UploadVideoChunk(
                    video_id=video_id,
                    data_chunk=chunk,
                    is_first_chunk=False
                )


@app.route('/upload', methods=['POST'])
def upload_video():
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file part'}), 400

    video_file = request.files['file']
    width = int(request.form.get('width', 640))
    height = int(request.form.get('height', 480))
    upscale_width = int(request.form.get('upscale_width', width))
    upscale_height = int(request.form.get('upscale_height', height))
    format = request.form.get('format', 'mp4')

    video_id = os.path.basename(video_file.filename)
    temp_path = os.path.join('uploads', video_id)
    os.makedirs('uploads', exist_ok=True)
    video_file.save(temp_path)

    try:
        with grpc.insecure_channel(GRPC_MASTER_ADDRESS) as channel:
            stub = replication_pb2_grpc.MasterServiceStub(channel)
            response = stub.UploadVideo(generate_chunks(
                temp_path, video_id, width, height, upscale_width, upscale_height, format))

        os.remove(temp_path)

        if response.success:
            return jsonify({'success': True, 'video_id': response.video_id})
        else:
            return jsonify({'success': False, 'message': response.message}), 500

    except grpc.RpcError as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/status/<video_id>', methods=['GET'])
def get_status(video_id):
    try:
        with grpc.insecure_channel(GRPC_MASTER_ADDRESS) as channel:
            stub = replication_pb2_grpc.MasterServiceStub(channel)
            request_pb = replication_pb2.VideoStatusRequest(video_id=video_id)
            response = stub.GetVideoStatus(request_pb)
            return jsonify({
                'success': True,
                'video_id': response.video_id,
                'status': response.status,
                'message': response.message
            })
    except grpc.RpcError as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/retrieve/<video_id>', methods=['GET'])
def retrieve_video(video_id):
    def generate():
        try:
            with grpc.insecure_channel(GRPC_MASTER_ADDRESS) as channel:
                stub = replication_pb2_grpc.MasterServiceStub(channel)
                request_pb = replication_pb2.RetrieveVideoRequest(video_id=video_id)
                response_stream = stub.RetrieveVideo(request_pb)

                for response in response_stream:
                    yield response.data_chunk

        except grpc.RpcError as e:
            print(f"[gRPC Retrieve Error] {e}")
            return  # Can't return an error JSON after headers sent

    return Response(
        generate(),
        mimetype='application/octet-stream',
        headers={
            'Content-Disposition': f'attachment; filename="{video_id}"'
        }
    )


if __name__ == '__main__':
    app.run(debug=True, port=8000)
