
# Video Encoding for CMPE275
Class Project for Distributed Video Encoding for SJSU CMPE 275

# ISSUES

Todo items are listed in Issues. Please contact Ignol in discord server for issue assignments. 

If there is a * next to the issue, that means it's a desired but not essential feature.

# Video Processing Worker Guide

This project implements a standalone Video Processing Worker using gRPC in Python. It's designed to receive video chunks from a Master process (simulated by the test client), simulate encoding them to make them smaller, and store them as local shards. It also provides an RPC to retrieve the stored shard files.

