#!/bin/bash
mainVersion=$(git rev-list HEAD --count).$(git rev-parse --short HEAD)

pravegaVersion=$(grep "pravegaVersion" gradle.properties | cut -d "=" -f2)
echo ${mainVersion}-${pravegaVersion}