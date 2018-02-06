FROM python:3.6.3
ADD ./ /opt/parser
WORKDIR /opt/parser
EXPOSE 3434
CMD python rest/rest.py && python Scripts/preprocess.py