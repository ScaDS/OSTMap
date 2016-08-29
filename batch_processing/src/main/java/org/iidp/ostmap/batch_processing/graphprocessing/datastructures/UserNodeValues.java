package org.iidp.ostmap.batch_processing.graphprocessing.datastructures;

import javax.annotation.Nonnull;

/**
 * Simple POJO for user based graph nodes
 */
public class UserNodeValues implements Comparable<UserNodeValues>{

    public final String userId;
    public final String userName;

    public UserNodeValues(@Nonnull String userId, String userName) {
        this.userId = userId;
        this.userName = userName;
    }

    @Override
    public int compareTo(@Nonnull final UserNodeValues o) {
        return this.userId.compareTo(o.userId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserNodeValues that = (UserNodeValues) o;

        return userId.equals(that.userId);

    }

    @Override
    public int hashCode() {
        return userId.hashCode();
    }

    @Override
    public String toString() {
        return "UserNodeValues{" +
                "userId=" + userId +
                ", userName='" + userName + '\'' +
                '}';
    }
}
