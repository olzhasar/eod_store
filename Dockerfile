FROM python:3.6

ENV CELERY_BROKER_URL redis://redis:6379/0
ENV CELERY_RESULT_BACKEND redis://redis:6379/0
ENV C_FORCE_ROOT true

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

RUN mkdir data/
RUN touch data/symbols

COPY /api /api

EXPOSE 5000

RUN pip install gunicorn

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "3", "api.wsgi:app"]
