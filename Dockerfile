FROM public.ecr.aws/amazonlinux/amazonlinux:2023

LABEL maintainer="EvanPFWang@gmail.com"
LABEL description="Container image for NBA DataUpdater"

# Install Python 3.11 explicitly (python3 on AL2023 is still 3.9)
RUN yum -y update && \
    yum -y install python3.11 python3.11-pip && \
    yum clean all

WORKDIR /app

COPY requirements.txt .
RUN python3.11 -m pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENTRYPOINT ["python3.11", "src/data_updater.py"]