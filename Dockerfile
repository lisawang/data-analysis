FROM songgithub/python_pandas:3.6

ADD . /app

RUN pip install -r requirements.txt

