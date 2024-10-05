FROM python:3-slim
WORKDIR /programas/ingesta
RUN pip3 install boto3 pymongo
COPY . .
RUN mkdir /programas/ingesta/output_data
CMD["python3", "./ingesta.py"]