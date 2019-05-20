FROM python:3
# Test Commen
WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

#CMD ["python", "./load_defs.py"]
CMD [ "python", "./run_maindef.py" ]
