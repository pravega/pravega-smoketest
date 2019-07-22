FROM devops-repo.isus.emc.com:8116/nautilus/base:27.7865490
ADD bin/ /app/bin/
ADD conf/ /app/conf/

ARG BUILD_VERSION
ADD pravega-smoketest-${BUILD_VERSION}-all.jar /app/lib/pravega-smoketest.jar