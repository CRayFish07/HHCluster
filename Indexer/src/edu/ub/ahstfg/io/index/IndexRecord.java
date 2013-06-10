/*
 * IndexRecord.java is part of HHCluster.
 *
 * HHCluster is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * HHCluster is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with HHCluster.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.ub.ahstfg.io.index;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Interface index record objects. That objects are the Indexer output.
 * @author Alberto Huelamo Segura
 */
public interface IndexRecord extends Writable {
    
    /**
     * Indicates if a record is a document record.
     * @return true if the record represents a document.
     */
    public boolean isDocument();
    
    /**
     * Writes in a stream information.
     * @param out Stream to write.
     * @throws IOException Thrown if there is a problem with the communication.
     */
    public void writeOutput(DataOutputStream out) throws IOException;
    
}
