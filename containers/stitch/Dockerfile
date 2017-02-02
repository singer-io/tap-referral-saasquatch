FROM 218546966473.dkr.ecr.us-east-1.amazonaws.com/stitch-orchestrator:0.0.4

RUN mkdir /code/stream-referral-saasquatch
WORKDIR /code/stream-referral-saasquatch
ADD . /code/stream-referral-saasquatch

RUN python3 setup.py install

CMD stitch-orchestrator \
  --streamer stream-referral-saasquatch \
  --persister persist-stitch \
  --client-id $STITCH_CLIENT_ID \
  --connection-id $STITCH_CONNECTION_ID
