import os
import subprocess
import webbrowser
import threading  
from flask import Flask, render_template, request, redirect, url_for, send_file

app = Flask(__name__)

# 爬蟲程式的位置和結果儲存位置
SPIDER_SCRIPT_PATH = 'C:\Users\T14 Gen 3\airflow-demo\tasks\test.py'
OUTPUT_CSV_PATH = 'C:\Users\T14 Gen 3\airflow-demo\data\output.csv'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run_spider', methods=['POST'])
def run_spider():
    # 不會copy到重覆的
    if os.path.exists(OUTPUT_CSV_PATH):
        os.remove(OUTPUT_CSV_PATH)

    # 爬蟲
    result = subprocess.run(['python', SPIDER_SCRIPT_PATH], capture_output=True, text=True)

    print(result.stdout)
    print(result.stderr)

    # 確認 CSV 是否產生
    if os.path.exists(OUTPUT_CSV_PATH):
        # 回傳下載連結
        return redirect(url_for('download_csv'))
    else:
        return "爬蟲未成功或未生成CSV文件", 500

@app.route('/download_csv')
def download_csv():
    return send_file(OUTPUT_CSV_PATH, as_attachment=True)

def open_browser():
    # 把網頁打開(類似彈出視窗)
    webbrowser.open("http://127.0.0.1:5000")

if __name__ == '__main__':
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Thread(target=open_browser).start()
    app.run(debug=True)
