```bash
python -m venv venv

# On Windows
venv\Scripts\activate

# On macOS/Linux
source venv/bin/activate
```

```bash
pip install -r requirements.txt
```

```bash
python main.py 1 localhost:5002 localhost:5003

python main.py 2 localhost:5001 localhost:5003

python main.py 3 localhost:5001 localhost:5002
```
