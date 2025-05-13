python node.py --role worker --host localhost --port 50061 --master localhost:50051 --nodes localhost:50051 localhost:50062 localhost:50063
python node.py --role worker --host localhost --port 50062 --master localhost:50051 --nodes localhost:50051 localhost:50061 localhost:50063
python node.py --role worker --host localhost --port 50063 --master localhost:50051 --nodes localhost:50051 localhost:50061 localhost:50062

