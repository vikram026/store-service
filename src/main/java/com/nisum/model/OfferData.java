package com.nisum.model;


import java.util.Objects;


public class OfferData
{
    private long offerId;
    private String offerType;
    private String storeId;
    private String terminal;

    public OfferData() {
    }

    @Override
    public String toString() {
        return "Offer{" +
                "offerId=" + offerId +
                ", offerType='" + offerType + '\'' +
                ", storeId='" + storeId + '\'' +
                ", terminal='" + terminal + '\'' +
                ", id='" + id + '\'' +
                ", preCondition='" + preCondition + '\'' +
                '}';
    }

    public long getOfferId() {
        return offerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OfferData that = (OfferData) o;
        return offerId == that.offerId &&
                Objects.equals(offerType, that.offerType) &&
                Objects.equals(storeId, that.storeId) &&
                Objects.equals(terminal, that.terminal) &&
                Objects.equals(id, that.id) &&
                Objects.equals(preCondition, that.preCondition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offerId, offerType, storeId, terminal, id, preCondition);
    }

    public void setOfferId(long offerId) {
        this.offerId = offerId;
    }

    public String getOfferType() {
        return offerType;
    }

    public void setOfferType(String offerType) {
        this.offerType = offerType;
    }

    public String getStoreId() {
        return storeId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public String getTerminal() {
        return terminal;
    }

    public void setTerminal(String terminal) {
        this.terminal = terminal;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPreCondition() {
        return preCondition;
    }

    public void setPreCondition(String preCondition) {
        this.preCondition = preCondition;
    }

    private String id;
    private String preCondition;

    public OfferData(long offerId, String offerType, String storeId, String terminal, String id, String preCondition) {
        this.offerId = offerId;
        this.offerType = offerType;
        this.storeId = storeId;
        this.terminal = terminal;
        this.id = id;
        this.preCondition = preCondition;
    }
}
