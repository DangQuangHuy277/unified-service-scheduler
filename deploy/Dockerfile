# One image recipe for both services: --build-arg PROJECT=Scheduler.Api | Scheduler.Worker
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG PROJECT
WORKDIR /src

# Restore first, from the project files only, so code changes don't invalidate the package layer.
COPY global.json nuget.config Directory.Build.props Directory.Build.targets Directory.Packages.props ./
COPY src/Scheduler.Domain/Scheduler.Domain.csproj src/Scheduler.Domain/
COPY src/Scheduler.Infrastructure/Scheduler.Infrastructure.csproj src/Scheduler.Infrastructure/
COPY src/Scheduler.Api/Scheduler.Api.csproj src/Scheduler.Api/
COPY src/Scheduler.Worker/Scheduler.Worker.csproj src/Scheduler.Worker/
RUN dotnet restore src/${PROJECT}/${PROJECT}.csproj

COPY src/ src/
RUN dotnet publish src/${PROJECT}/${PROJECT}.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG PROJECT
ENV APP=${PROJECT}
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
# exec keeps dotnet as PID 1 so it receives SIGTERM and shuts down cleanly (workers leave the Kafka group).
ENTRYPOINT ["/bin/sh", "-c", "exec dotnet \"$APP.dll\" \"$@\"", "--"]
