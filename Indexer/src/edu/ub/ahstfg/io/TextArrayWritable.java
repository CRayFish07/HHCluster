/*
 * TextArrayWritable.java is part of HHCluster.
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

package edu.ub.ahstfg.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

@Deprecated
public class TextArrayWritable extends ArrayWritable {
    
    public TextArrayWritable() {
        super(Text.class);
    }
    
    public TextArrayWritable(Text[] values) {
        super(Text.class, values);
    }
    
    public TextArrayWritable(String[] values) {
        super(Text.class);
        Text[] vt = new Text[values.length];
        for (int i = 0; i < values.length; i++) {
            vt[i] = new Text(values[i]);
        }
        super.set(vt);
    }
    
}
