###########################################
###########################################
## Dockerfile to run Validate-DAG-Config ##
###########################################
###########################################

#########################################
## Container image that runs your code ##
#########################################
FROM alpine:3.10

#########################
# Get base python image #
#########################
FROM python:3.10.4-alpine as base_image

#############################
# Copy scripts to container #
#############################
COPY lib /action/lib

######################
# Set the entrypoint #
######################
ENTRYPOINT ["/action/lib/validate.sh"]