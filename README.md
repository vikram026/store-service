#EPE-005 : offerlookup-div-kafka
    Project to consume OfferLookUpData from look-up-kafka., process data and post data to Division Kafka with storeId as topic
    git-url	https://gitlab.mynisum.com/vksingh/create-store-service.git - clone the project
    
    


**Prerequisite:**
   *    Confluent kafka set up in machine
    
    
#Offer Creation In Confluent Kafka

    TestDataGenerator - generatorTestData()   generates the OfferLookUp to  "offerLkpConnect" Topic

# Application

    Running the main application triggers the spark job to consume the data from offerLkpConnect - topic
    flatten the data and push the offer to store topic / Division kafka
# Tests
    OfferLookUpToDivisionIntTest does the whole job as :
    *   Generates the random data with store information to "offerLkpConnect"
    *   Triggers the flattening logic
    *   Pushes data to division kafka
    *   Asserts the messages in division kafka with the offer data from lookUp kafka