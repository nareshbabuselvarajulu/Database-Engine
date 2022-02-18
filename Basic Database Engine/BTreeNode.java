import java.sql.SQLException;

/**
 * Created by shruti on 20/4/17.
 */
//class BTreeNode<Element,E extends Comparable<E1>, E1> implements Comparable<E1>{
class BTreeNode{
    //BTreeNode left, right;
    int order;
    Element newElement;
    Element data[] = null;
    Element elements[] = null;
    BTreeNode children[];
    Element dataElement;


    public BTreeNode(int order){
        this.order = order;
        //final T[] data = (T[]) Array.newInstance(c, order);
        Element[] elements1 = new Element[order];
        this.elements = elements1;
        BTreeNode[] children = new BTreeNode[order+1];
        this.children = children;
    }


    public BTreeNode(){
    }

    public void assignValue(Element currentElement){
        this.dataElement = currentElement;
    }

    public int compareTo(Element e) throws SQLException {
        // TODO Auto-generated method stub
//        return ((Comparable<E1>) dataElement).compareTo(e);
        return dataElement.compareTo(e.value);
    }

}
