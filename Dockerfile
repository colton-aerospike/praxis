FROM golang:1.19-alpine


RUN mkdir -p /opt/praxis
WORKDIR /opt/praxis
COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
RUN GOOS=linux GOARCH=amd64 go build -o /opt/praxis/praxis
ENV TZ=America/New_York

RUN GOOS=linux GOARCH=amd64 go build -o /opt/praxis/praxis
RUN apk add --no-cache tzdata



#CMD [ "/opt/praxis/praxis -h aerocluster-0-0.aerocluster.aerospike -U admin -P admin123 -k 1000 -n test -s myset -chan 750 -sT 50ms -q -iB mapBin -iV 8675309" ] 

ENTRYPOINT /opt/praxis/praxis -h k8s-virt-1 -U admin -P admin123 -k 10000 -n test -s myset -chan 500 -sT 10ms -q -iB mapBin -iV 8675309
