from mhart/alpine-node:4

RUN apk --update add git
ADD package.json package.json
RUN npm install

ADD lib/ lib/
ADD test/ test/

RUN npm run test
