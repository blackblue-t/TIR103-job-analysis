FROM apache/airflow:latest

# 安裝所需的 Python 套件
USER airflow
RUN pip install tqdm pandas numpy logging pathlib beautifulsoup4 urllib3 datetime jieba selenium webdriver-manager

# 切換到 root 使用者來安裝 Chrome 和 ChromeDriver
USER root
RUN apt-get update && \
    apt-get install -y wget unzip libnss3 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxdamage1 libxi6 \
                       libxtst6 libnss3 libxrandr2 libasound2 libatk1.0-0 libgtk-3-0 libxss1 fonts-liberation \
                       libappindicator3-1 libxshmfence1 xdg-utils


# 下載並安裝固定版本的 Chrome (130.0.6723.116)
RUN wget -q https://storage.googleapis.com/chrome-for-testing-public/130.0.6723.116/linux64/chrome-linux64.zip && \
    unzip chrome-linux64.zip -d /opt/google/ && \
    ln -s /opt/google/chrome-linux64/chrome /usr/bin/google-chrome && \
    rm chrome-linux64.zip

# 移除任何現有的 ChromeDriver
RUN rm -f /usr/local/bin/chromedriver

# 下載並安裝對應的 ChromeDriver 版本 (130.0.6723.116)
RUN wget -q https://storage.googleapis.com/chrome-for-testing-public/130.0.6723.116/linux64/chromedriver-linux64.zip && \
    unzip chromedriver-linux64.zip -d /usr/local/bin/ && \
    mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf chromedriver-linux64.zip /usr/local/bin/chromedriver-linux64

# 設置權限並切換回 airflow 使用者
USER airflow