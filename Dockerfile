FROM golang:1.19-alpine


RUN mkdir -p /opt/praxis/udf
WORKDIR /opt/praxis
COPY go.mod go.sum ./
RUN go mod download

COPY ./udf ./udf
COPY main.go ./
ENV TZ=America/New_York

RUN GOOS=linux GOARCH=amd64 go build -o /opt/praxis/praxis
RUN apk add --no-cache tzdata



#CMD [ "/opt/praxis/praxis -h aerocluster-0-0.aerocluster.aerospike -U admin -P admin123 -k 1000 -n test -s myset -chan 750 -sT 50ms -q -iB mapBin -iV 8675309" ] 

ENTRYPOINT /opt/praxis/praxis -h ${PRAXIS_HOST} -U ${PRAXIS_USER} -P ${PRAXIS_PASS} -k ${PRAXIS_KEY_COUNT} -n ${PRAXIS_NAMESPACE} -s ${PRAXIS_SETNAME} -chan ${PRAXIS_CHANNEL_SIZE} -sT ${PRAXIS_SLEEP_TIMER} -q -iB mapBin -iV 8675309
