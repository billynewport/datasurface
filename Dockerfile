FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's source code
COPY . .

# Install the project
RUN pip install .

# TODO: Replace this with the actual command to run your application
CMD ["python", "-m", "src.datasurface.main"] 