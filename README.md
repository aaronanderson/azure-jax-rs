azure-jax-rs
----

Simple Java [JAX-RS](http://en.wikipedia.org/wiki/Java_API_for_RESTful_Web_Services) 2.0 Client Utility library that can invoke REST services in the Microsoft [Azure](http://azure.microsoft.com/en-us/) Platform as a Service [Paas](http://en.wikipedia.org/wiki/Platform_as_a_service) environment

Microsoft already provides robust full featured Java API libraries for the Azure REST services. However the implementations vary (JDK HTTPURLConnection for storage, Apache HTTPClient for DocumentDB) and they all have various additional third party dependencies. The intent of this project is to use the standard Java API for REST services, JAX-RS, to interact with the Azure environment with minimal dependencies. 

This project uses Oracle Jersey, the JAX-RS reference implemention, as the JAX-RS implementation along with the GlassFish RI for JSR-353 Java JSON processing. However this project does not use any Jersey specific extensions so any compliant JAX-RS 2.0 client implemention should be compatible.

The objective of this project is to provide a minimal interface to Azure and performance has not be tested or tuned. With the JAX-RS client API building a WebTarget that contains multiple components causes many new object copies which may cause performance issues. Also HTTP keep-alive connection re-use has not been confirmed. 

Finally debugging JAX-RS HTTP invocations at the wire level is surprising challenging. While the default Jersey HTTP client implementation uses the JDK HTTPURLConnection implementation which uses JDK logging I could not find the magic logging properties to dump wire traffic out to the maven surefire test logs. Eventually I used the Man in the Middle proxy, mitmproxy, for HTTP tracing of DocumentDB and Storage.

----
## Windows Azure Setup

1. Create an Azure account. 

2. Once the account is registered create Storage , DocumentDB, and free Search accounts. Please record the name of each service which is used in the hostname used to access the service. For example, when a new Storage account is created named teststorage the hostname used to access the account would be teststorage.core.windows.net. This name will be used as the clientId value in this utility library.

3. For DocumentDB and Storage primary and secondary keys are provided for the accounts. Please record the primary key values as they will be used as the MasterKey in Storage and DocumentDB utility classes. For search primary and secondary secret API keys are used and the primary API key is used as the MasterKey in the search utility class.

4. Please examine the [JUnit tests](../master/src/test/java/com/cpsgpartners/azure) to see how the utility classes can be invoked. Before running the tests please plug in the correct CLIENT_ID and MASTER_KEY values into the variables at the top of the JUnit tests.

----
