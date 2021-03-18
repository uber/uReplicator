  rm ca-* cert-* *.jks || true

  export PASS=foobar

  keytool -keystore kafka.server.keystore.jks -alias localhost -validity 365 -genkey -storepass $PASS -keypass $PASS -noprompt -dname "CN=localhost, OU=EE, O=DD, L=CC, S=AA, C=BB"

  openssl req -new -x509 -keyout ca-key -out ca-cert -days 365 -passout pass:$PASS -subj '/CN=www.myurep.com/O=MyURep/C=US'
  keytool -keystore kafka.server.truststore.jks -alias CARoot -import -file ca-cert -storepass $PASS -noprompt
  keytool -keystore kafka.server.keystore.jks -alias localhost -certreq -file cert-file -storepass $PASS -noprompt

  openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 365 -CAcreateserial -passin pass:$PASS

  keytool -keystore kafka.server.keystore.jks -alias CARoot -import -file ca-cert -storepass $PASS -noprompt
  keytool -keystore kafka.server.keystore.jks -alias localhost -import -file cert-signed -storepass $PASS -noprompt
  keytool -keystore kafka.client.truststore.jks -alias CARoot -import -file ca-cert -storepass $PASS -noprompt

  rm ca-* cert-*