/*
 * JBoss DNA (http://www.jboss.org/dna)
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * See the AUTHORS.txt file in the distribution for a full listing of 
 * individual contributors.
 *
 * JBoss DNA is free software. Unless otherwise indicated, all code in JBoss DNA
 * is licensed to you under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 * 
 * JBoss DNA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.dna.graph.query.validate;

import net.jcip.annotations.Immutable;
import org.jboss.dna.graph.property.PropertyType;
import org.jboss.dna.graph.query.validate.Schemata.Column;

@Immutable
class ImmutableColumn implements Column {

    public static final boolean DEFAULT_FULL_TEXT_SEARCHABLE = false;

    private final boolean fullTextSearchable;
    private final String name;
    private final PropertyType type;

    protected ImmutableColumn( String name,
                               PropertyType type ) {
        this(name, type, DEFAULT_FULL_TEXT_SEARCHABLE);
    }

    protected ImmutableColumn( String name,
                               PropertyType type,
                               boolean fullTextSearchable ) {
        this.name = name;
        this.type = type != null ? type : PropertyType.STRING;
        this.fullTextSearchable = fullTextSearchable;
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.jboss.dna.graph.query.validate.Schemata.Column#getName()
     */
    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.jboss.dna.graph.query.validate.Schemata.Column#getPropertyType()
     */
    public PropertyType getPropertyType() {
        return type;
    }

    /**
     * {@inheritDoc}
     * 
     * @see org.jboss.dna.graph.query.validate.Schemata.Column#isFullTextSearchable()
     */
    public boolean isFullTextSearchable() {
        return fullTextSearchable;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return this.name + "(" + type.getName() + ")";
    }
}