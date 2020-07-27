package T_221B;
import java.awt.Dimension;
import java.awt.Polygon;
import java.awt.geom.Point2D;

public class polygon extends Polygon implements Comparable<polygon>{
	 protected Point2D.Double[] vertices;
	
	public double area()
	  {
		int n = vertices.length;
		int sum =0;
		for (int i = 0; i < n; i++) {
		    sum += vertices[i].x * (vertices[(i + 1) % n].y - vertices[(i + n - 1) % n].y);
		}
	    double area = 0.5 * Math.abs(sum);
	    return area;

	  }
	public polygon(int[] x, int[] y, int i) {
		this.xpoints=x;
		this.ypoints=y;
		this.npoints=i;
		vertices=new Point2D.Double[npoints];
		for(int j=0;j<npoints;j++) {
			vertices[j]=new Point2D.Double(x[j],y[j]);
		}
	}

	public int compareTo(polygon poly1) {
	Double nThisArea =  this.area();
	Double nThisArea2 =  poly1.area();

	 return nThisArea.compareTo(nThisArea2);
	}
	
	public int getArea() {
		Dimension dimension2 =  this.getBounds().getSize();
		Integer nThisArea2 = dimension2.width*dimension2.height;
		return nThisArea2;
	}
	
	public String toString() {     ///2aih length should i put it  (aya abdalla)
		String x="";
				for(int i=0; i<this.xpoints.length;i++) {
					x+=Integer.toString(this.xpoints[i]);
				}
				String y="";
				for(int i=0; i<this.ypoints.length;i++) {
					y+=Integer.toString(this.ypoints[i]);
				}
				
		return x+y;									
	}
	
}