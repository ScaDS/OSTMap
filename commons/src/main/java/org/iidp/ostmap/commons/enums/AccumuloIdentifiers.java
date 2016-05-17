package org.iidp.ostmap.commons.enums;


public enum AccumuloIdentifiers {
    PROPERTY_INSTANCE{
        public String toString(){
            return "accumulo.instance";
        }
    },
    PROPERTY_USER{
        public String toString(){
            return "accumulo.user";
        }
    },
    PROPERTY_PASSWORD{
        public String toString(){
            return "accumulo.password";
        }
    },
    PROPERTY_ZOOKEEPER{
        public String toString(){
            return "accumulo.zookeeper";
        }
    }
}
