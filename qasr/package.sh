#!/bin/bash
rm -f dbproxy.tar.gz
mv target dbproxy
chmod 755 dbproxy/bin/startup.sh
tar cvfz dbproxy.tar.gz dbproxy/bin/ dbproxy/conf/ dbproxy/lib/
rm -rf dbproxy
