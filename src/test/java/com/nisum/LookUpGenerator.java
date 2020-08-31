package com.nisum;

import com.genericAvro.OfferLookupData;
import com.nisum.model.OfferData;
import com.nisum.repository.StoreRepository;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LookUpGenerator {

    static Random random = new Random();

    public static List<OfferLookupData> generateLookUpData(int limit) {

        return IntStream.range(0, limit).mapToObj(offerId -> {
            OfferData offerLookup = new OfferData();
            offerLookup.setOfferId(offerId);

            if (StoreRepository.customerGroup.length > offerId) {
                offerLookup.setId(StoreRepository.customerGroup[offerId]);
                offerLookup.setOfferType("CONSUMER");
            } else {
                int consumerId = offerId % StoreRepository.customerGroup.length;
                offerLookup.setOfferType("UPC");
                offerLookup.setId(StoreRepository.productGroup[consumerId]);
            }

            int consumerIdForStore = random.nextInt(StoreRepository.store.length);
            offerLookup.setStoreId(StoreRepository.store[consumerIdForStore]);

            offerLookup.setPreCondition("AND");
            if (StoreRepository.terminals.length > offerId) {
                offerLookup.setTerminal(StoreRepository.terminals[offerId]);
            } else {
                int consumerId = offerId % StoreRepository.terminals.length;
                offerLookup.setTerminal(StoreRepository.terminals[consumerId]);
            }
            return mapOffer(offerLookup);
        }).collect(Collectors.toList());

    }

    private static OfferLookupData mapOffer(OfferData offer) {
        return OfferLookupData.newBuilder()
                .setOfferId(offer.getOfferId())
                .setOfferType(offer.getOfferType())
                .setId(offer.getId())
                .setPreCondition(offer.getPreCondition())
                .setStoreId(offer.getStoreId())
                .setTerminal(offer.getTerminal()).build();
    }
}
