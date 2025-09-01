FROM python:3.11-slim

WORKDIR /app


COPY requirements.txt .
RUN pip install -r requirements.txt

ENV NLTK_DATA=/usr/local/share/nltk_data
RUN python -m nltk.downloader -d /usr/local/share/nltk_data vader_lexicon

COPY . .

CMD ["python", "-m", "main"]
