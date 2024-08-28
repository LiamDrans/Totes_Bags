# Use the official Python runtime as the base image
FROM public.ecr.aws/lambda/python:3.9

# Set the working directory in the container
WORKDIR /app

# Install any necessary dependencies
RUN apt-get update
RUN apt-get install -y zip 
RUN rm -rf /var/lib/apt/lists/*

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install the Python packages listed in requirements.txt
RUN pip install -r requirements.txt -t /opt/python/

# Set the CMD to zip the installed packages into a layer
# change the `requests-layer` to the LAYER_NAME variable as per create_layer.sh file

CMD cd /opt && zip -r9 /app/requests-layer.zip .