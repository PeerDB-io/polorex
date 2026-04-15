FROM golang:1.24 AS build

ADD --checksum=sha256:05c6b4a9bf4daa95c888711481e19cc8e2e11aed4de8db759f51f4a6fce64917 https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /usr/local/share/ca-certificates/global-aws-rds-bundle.pem
RUN dpkg-reconfigure ca-certificates

FROM gcr.io/distroless/static-debian11:debug
# debug image contains busybox needed in build steps
SHELL ["/busybox/sh", "-c"]
COPY --from=build /etc/ssl/certs /etc/ssl/certs
