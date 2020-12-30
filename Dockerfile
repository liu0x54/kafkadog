FROM golang:latest as builder



WORKDIR /root


COPY cmd/kafkadog.go /root/

#RUN mkdir /usr/lib/ld

WORKDIR /root/
RUN go mod init kafkadog.com/m/v2

RUN go build -o kafkadog 


FROM ubuntu


WORKDIR /root

COPY --from=builder /root/kafkadog /root/
WORKDIR /root
CMD ["kafkadog"]