import os
import sys
import tempfile
from pathlib import Path

# 让测试可导入 app 包；DATA_DIR 指到临时目录，避免碰本地开发库
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="clickspan-test-"))
