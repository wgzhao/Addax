/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.reader.elasticsearchreader;

import ognl.MemberAccess;
import ognl.OgnlContext;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Member;
import java.lang.reflect.Modifier;
import java.util.Map;

/**
 * This class provides methods for setting up and restoring
 * access in a Field.  Java 2 provides access utilities for setting
 * and getting fields that are non-public.  This object provides
 * coarse-grained access controls to allow access to private, protected
 * and package protected members.  This will apply to all classes
 * and members.
 *
 * @author Luke Blanshard (blanshlu@netscape.net)
 * @author Drew Davidson (drew@ognl.org)
 * @version 15 October 1999
 */
public class DefaultMemberAccess
        implements MemberAccess
{

    public boolean allowPrivateAccess = false;
    public boolean allowProtectedAccess = false;
    public boolean allowPackageProtectedAccess = false;

    /*===================================================================
        Constructors
      ===================================================================*/
    public DefaultMemberAccess(boolean allowAllAccess)
    {
        this(allowAllAccess, allowAllAccess, allowAllAccess);
    }

    public DefaultMemberAccess(boolean allowPrivateAccess, boolean allowProtectedAccess, boolean allowPackageProtectedAccess)
    {
        super();
        this.allowPrivateAccess = allowPrivateAccess;
        this.allowProtectedAccess = allowProtectedAccess;
        this.allowPackageProtectedAccess = allowPackageProtectedAccess;
    }

    /*===================================================================
        Public methods
      ===================================================================*/
    public boolean getAllowPrivateAccess()
    {
        return allowPrivateAccess;
    }

    public void setAllowPrivateAccess(boolean value)
    {
        allowPrivateAccess = value;
    }

    public boolean getAllowProtectedAccess()
    {
        return allowProtectedAccess;
    }

    public void setAllowProtectedAccess(boolean value)
    {
        allowProtectedAccess = value;
    }

    public boolean getAllowPackageProtectedAccess()
    {
        return allowPackageProtectedAccess;
    }

    public void setAllowPackageProtectedAccess(boolean value)
    {
        allowPackageProtectedAccess = value;
    }

    /*===================================================================
        MemberAccess interface
      ===================================================================*/
    public Object setup(OgnlContext ognlContext, Object o, Member member, String s)
    {
        Object result = null;

        if (isAccessible(ognlContext, o, member, s)) {
            AccessibleObject accessible = (AccessibleObject) member;

            if (!accessible.isAccessible()) {
                result = Boolean.FALSE;
                accessible.setAccessible(true);
            }
        }
        return result;
    }

    public void restore(OgnlContext ognlContext, Object o, Member member, String s, Object o1)
    {
        if (o1 != null) {
            ((AccessibleObject) member).setAccessible((Boolean) o1);
        }
    }

    /**
     * Returns true if the given member is accessible or can be made accessible
     * by this object.
     *
     * @param ognlContext the current execution context (not used).
     * @param o the Object to test accessibility for (not used).
     * @param member the Member to test accessibility for.
     * @param s the property to test accessibility for (not used).
     * @return true if the member is accessible in the context, false otherwise.
     */
    public boolean isAccessible(OgnlContext ognlContext, Object o, Member member, String s)
    {
        int modifiers = member.getModifiers();
        boolean result = Modifier.isPublic(modifiers);

        if (!result) {
            if (Modifier.isPrivate(modifiers)) {
                result = getAllowPrivateAccess();
            }
            else {
                if (Modifier.isProtected(modifiers)) {
                    result = getAllowProtectedAccess();
                }
                else {
                    result = getAllowPackageProtectedAccess();
                }
            }
        }
        return result;
    }
}
