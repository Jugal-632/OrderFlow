ARG BUILD_IMAGE=eclipse-temurin:21-jdk-jammy@sha256:4b1a74fbffbe6b4e7b6f0171a472478282fb463790a51edf10e465d578f22b69
ARG RUNTIME_IMAGE=eclipse-temurin:21-jre-jammy@sha256:1eac8b8fee11ca7d6725a48bc5d41992ed1561504ebae25490d2d062847e8269
FROM ${BUILD_IMAGE} AS builder
WORKDIR /src
COPY . .
RUN --mount=type=cache,id=orderflow-maven,target=/root/.m2,sharing=locked chmod +x mvnw && ./mvnw -q -B package -DskipTests
RUN javac -d /health containers/Healthcheck.java
FROM ${RUNTIME_IMAGE}
# Patch CVE-2026-109518 in the pinned Jammy base; keep the security gate enabled.
# Disable retries while bootstrapping the vulnerable APT client.
RUN apt-get -o Acquire::Retries=0 update \
    && apt-get -o Acquire::Retries=0 install -y --no-install-recommends \
       apt=2.4.15ubuntu0.1 libapt-pkg6.0=2.4.15ubuntu0.1 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN mkdir -p /app/logs && chown -R 10001:10001 /app
COPY --from=builder /src/api-gateway/target/app.jar /app/app.jar
COPY --from=builder /health /app/health
USER 10001:10001
ENV JAVA_TOOL_OPTIONS="-Duser.timezone=Asia/Kolkata -XX:MaxRAMPercentage=65"
HEALTHCHECK --interval=10s --timeout=5s --start-period=90s --retries=12 CMD ["java","-cp","/app/health","Healthcheck"]
ENTRYPOINT ["java","-jar","/app/app.jar"]
