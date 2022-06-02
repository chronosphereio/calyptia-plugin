FROM golang:latest AS builder

# These should change very infrequently and are coupled tightly
ARG CMETRICS_VERSION=0.3.3
ENV CMETRICS_VERSION=${CMETRICS_VERSION}
ARG CMETRICS_RELEASE=v0.3.3
ENV CMETRICS_RELEASE=${CMETRICS_RELEASE}

ARG PACKAGEARCH=amd64
ENV PACKAGEARCH=${PACKAGEARCH}

WORKDIR /fluent-bit-go

COPY go.mod .
COPY go.sum .

RUN go mod download
RUN go mod verify

COPY . .

ADD https://github.com/calyptia/cmetrics/releases/download/${CMETRICS_RELEASE}/cmetrics_${CMETRICS_VERSION}_${PACKAGEARCH}-headers.deb external/
ADD https://github.com/calyptia/cmetrics/releases/download/${CMETRICS_RELEASE}/cmetrics_${CMETRICS_VERSION}_${PACKAGEARCH}.deb external/
RUN dpkg -i external/*.deb

RUN go build -trimpath -buildmode c-shared -o ./bin/go-test-input-plugin.so ./testdata/input/input.go
RUN go build -trimpath -buildmode c-shared -o ./bin/go-test-output-plugin.so ./testdata/output/output.go

FROM ghcr.io/calyptia/calyptia-fluent-bit/advanced:main

COPY --from=builder /fluent-bit-go/bin/go-test-input-plugin.so /fluent-bit/etc/
COPY --from=builder /fluent-bit-go/bin/go-test-output-plugin.so /fluent-bit/etc/

COPY ./testdata/fluent-bit.conf /fluent-bit/etc/
COPY ./testdata/plugins.conf /fluent-bit/etc/

ENTRYPOINT [ "/fluent-bit/bin/fluent-bit" ]
CMD [ "/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf" ]
