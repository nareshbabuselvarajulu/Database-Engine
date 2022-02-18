import net.sf.jsqlparser.expression.PrimitiveValue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

/**
 * Created by shruti on 20/4/17.
 */


class BTree {
    public BTreeNode root = null;
    int order;
    private boolean ifCurrentNodeFull;
    private int length;
    private Element middleElement;
    public BTreeNode leftNode;
    public BTreeNode rightNode;
//    public static ArrayList<Integer> indexValues = new ArrayList<>();
//    public static ArrayList<Integer> indexValuesDescending = new ArrayList<>();

    public BTree(int k) {
        order = k;
    }

    public BTree() {
    }


    public boolean ifLeafNode(BTreeNode current) {
        BTreeNode[] children = current.children;
        int i;
        for (i = 0; i < children.length; i++) {
            if (children[i] == null) {
                continue;
            } else {
                break;
            }
        }
        if (i == children.length) {
            return true;
        } else {
            return false;
        }
    }

    public BTree isNodeFull(BTreeNode current) {
        Element[] elements = current.elements;
        int j;
        int length = 0;
        boolean ifCurrentNodeFull = false;

        for (j = 0; j < elements.length; j++) {
            if (elements[j] != null) {
                continue;
            } else {
                length = j;
                break;
            }
        }
//        System.out.println(j);
        if (j == elements.length) {
            ifCurrentNodeFull = true;
            length = elements.length;
        }

        BTree btree2Function = new BTree();
        btree2Function.ifCurrentNodeFull = ifCurrentNodeFull;
        btree2Function.length = length;
        return btree2Function;

    }


    public void insertInNode(BTreeNode current, Element element, BTreeNode leftNode, BTreeNode rightNode, int position) throws SQLException {
        Element[] elements = (Element[]) current.elements;
        BTreeNode[] children = current.children;
        if(leftNode==null && rightNode==null){
            loop1: for(int i=0; i<elements.length; i++){
                Element currentStoredElement = elements[i];
//                System.out.println(currentStoredElement);
                BTreeNode bn = new BTreeNode();
                bn.assignValue(element);
                int comparisionResult = bn.compareTo(currentStoredElement);

//                System.out.println(comparisionResult);
                //after
                if(comparisionResult >=1){
                    int m = i+1;
                    if(elements[m]!=null){
                        continue loop1;
                    }
                    else{
                        elements[m] = element;
                        children[m] = leftNode;
                        children[m+1] = rightNode;
                        break loop1;
                    }
                }

                //before
                else{
                    Element originalElement = element;
                    loop2: while(i<elements.length && elements[i]!=null){
                        Element t = elements[i];
                        //BTreeNode tempLeft = children[i];
                        BTreeNode tempRight = children[i+1];
                        if(element.equals(originalElement)){
                            elements[i] = element;
                            children[i] = leftNode;
                            children[i+1] = rightNode;
                        }
                        else{
                            elements[i] = element;
                            children[i+1] = rightNode;
                        }
//                        System.out.println("shruti "+i);
                        int m = i+1;
                        if(elements[m]==null){
                            elements[m] = t;
                            children[m+1] = tempRight;
                            break loop1;
                        }
                        else{
                            element = t;
                            rightNode = tempRight;
                            i++;
                            continue loop2;
                        }

                    }
                }

            }
        }

        else{
            int i = position;
            if(elements[i]==null){
                elements[i] = element;
                children[i] = leftNode;
                children[i+1] = rightNode;
            }

            else{
                Element originalElement = element;

                loop3: while(i<elements.length){
                    Element t = elements[i];
                    //BTreeNode tempLeft = children[i];
                    BTreeNode tempRight = children[i+1];
                    if(element.equals(originalElement)){
                        elements[i] = element;
                        children[i] = leftNode;
                        children[i+1] = rightNode;
                    }
                    else{
                        elements[i] = element;
                        children[i+1] = rightNode;
                    }
//                    System.out.println("shruti "+i);
                    int m = i+1;
                    if(elements[m]==null){
                        elements[m] = t;
                        children[m+1] = tempRight;
                        break loop3;
                    }
                    else{
                        element = t;
                        rightNode = tempRight;
                        i++;
                    }

                }
            }

        }
//		for(int i=0; i<elements.length; i++){
//			System.out.println("elements "+elements[i]);
//		}
//
//		for(int i=0; i<children.length; i++){
//			if(!ifLeafNode(current)){
//				if(children[i]!=null){
//					Element[] ele = (Element[]) children[i].elements;
//					for(int j=0; j<ele.length; j++){
//						System.out.print("children: ");
//						System.out.print(ele[j]+" ");
//					}
//				}
//			}
//		}
    }






    public void splitNode(Stack nodeStack, BTreeNode current) throws SQLException {

        Element middleElement1 = null;
        BTreeNode leftNode1 = null;
        BTreeNode rightNode1 = null;
        boolean createNewRoot = false;
        int position;
        int index = 0;

        Element[] elements = current.elements;
        BTreeNode[] children = current.children;

        if (order % 2 == 0) {
            position = order / 2 - 1;
        } else {
            position = order / 2;
        }
        Element middleElement = elements[position];
        Element[] leftElements = new Element[order];
        Element[] rightElements = new Element[order];
        BTreeNode[] leftElementsChildren = new BTreeNode[order + 1];
        BTreeNode[] rightElementsChildren = new BTreeNode[order + 1];
        for (int i = 0; i < position; i++) {
            leftElements[i] = elements[i];
            leftElementsChildren[i] = children[i];
        }
        leftElementsChildren[position] = children[position];

        for (int i = position + 1; i < elements.length; i++) {
            rightElements[index] = elements[i];
            rightElementsChildren[index] = children[i];
            index++;
        }
        rightElementsChildren[index] = children[children.length - 1];

//        System.out.println("m " + middleElement);
//        for (int i = 0; i < leftElements.length; i++) {
//            System.out.println("l " + leftElements[i]);
//        }
//        for (int i = 0; i < rightElements.length; i++) {
//            System.out.println("r " + rightElements[i]);
//        }


        BTreeNode leftNode = new BTreeNode(order);
        leftNode.elements = leftElements;
        leftNode.children = leftElementsChildren;
//		for(int i=0; i< leftElements.length; i++){
//			leftElements[i].
//		}
        BTreeNode rightNode = new BTreeNode(order);
        rightNode.elements = rightElements;
        rightNode.children = rightElementsChildren;


        if (current.equals(root)) {
//            System.out.println("jaya");
            root = new BTreeNode(order);

            Element[] elements1 = root.elements;
            elements1[0] = middleElement;
            BTreeNode[] children1 = root.children;
            children1[0] = leftNode;    //left
            children1[1] = rightNode;    //right

        } else {
//            System.out.println("rima");
            if (!(nodeStack.isEmpty())) {
                BTreeNode topOfStack = (BTreeNode) nodeStack.pop();
                //check if topOFSstack is full

                //where in topOfStack lies your current
                BTreeNode[] children2 = topOfStack.children;
                int i;
                for(i=0; i< children2.length; i++){
                    if(children2[i].equals(current)){
                        break;
                    }
                }
                int pos = i;

//                System.out.println("one");
                insertInNode(topOfStack, middleElement, leftNode, rightNode, pos);
                BTree btree2Functions3 = isNodeFull(topOfStack);
                boolean ifCurrentNodeFull = btree2Functions3.ifCurrentNodeFull;

                if (ifCurrentNodeFull == true) {
//                    System.out.println("two");
                    splitNode(nodeStack, topOfStack);
                }
            }
        }
    }

    public void insert(Element element) throws SQLException {
        if (root == null) {
            //System.out.println("abc");
            root = new BTreeNode(order);

            Element[] elements = root.elements;
            elements[0] = element;
            BTreeNode[] children = root.children;
            children[0] = null;    //left
            children[1] = null;    //right

//            T[] temp1 = (T[]) root.children;
//            //printing root.data
//            for(int i=0; i<temp1.length; i++){
//                System.out.println(temp1[i]);
//            }
            return;
        } else {
            //System.out.println("def");
            BTreeNode current = root;
            Stack nodeStack = new Stack();

            int length = 0;
            boolean ifCurrentNodeFull = false;
            //length = btree2Functions1.length;

            if (ifLeafNode(current)) {
                //System.out.println("hey");
                insertInNode(current, element, null, null, -1);
                BTree btree2Functions1 = isNodeFull(current);
                ifCurrentNodeFull = btree2Functions1.ifCurrentNodeFull;
                if (ifCurrentNodeFull == true) {
                    splitNode(nodeStack, current);
                }
            } else {
                //System.out.println("hello");
                loop3:
                while (true) {
                    //System.out.println("ghi");
                    nodeStack.push(current);
                    BTree btree2Functions2 = isNodeFull(current);
                    length = btree2Functions2.length;
                    Element[] elements = current.elements;

                    loop4:
                    for (int i = 0; i < length; i++) {
                        Element currentStoredElement = elements[i];
//                        System.out.println("a " + currentStoredElement);
                        BTreeNode bn = new BTreeNode();
                        bn.assignValue(element);
                        int comparisionResult = bn.compareTo(currentStoredElement);


                        if (comparisionResult >= 1) {
                            if (i < length - 1) {
                                //System.out.println("ek");
                                int m = i + 1;
                                Element nextStoredElement = elements[m];
//                                System.out.println(nextStoredElement);
                                BTreeNode btreeNode = new BTreeNode();
                                btreeNode.assignValue(element);
                                int comparisionResult1 = btreeNode.compareTo(nextStoredElement);
                                if (comparisionResult1 < 1) {
                                    BTreeNode[] children = current.children;
                                    current = children[i + 1];
                                    //nodeStack.push(current);
                                    if (ifLeafNode(current)) {
                                        //System.out.println("ek "+ ifLeafNode(current));
                                        insertInNode(current, element, null, null, -1);
                                        BTree btree2Functions3 = isNodeFull(current);
                                        ifCurrentNodeFull = btree2Functions3.ifCurrentNodeFull;
                                        if (ifCurrentNodeFull == true) {
                                            splitNode(nodeStack, current);
                                        }
                                        break loop3;
                                    } else {
                                        break loop4;
                                    }
                                }
                            } else {
                                //System.out.println("do");
                                BTreeNode[] children = current.children;
                                current = children[i + 1];
                                //nodeStack.push(current);
                                if (ifLeafNode(current)) {
                                    //System.out.println("do "+ ifLeafNode(current));
                                    insertInNode(current, element, null, null, -1);
                                    BTree btree2Functions3 = isNodeFull(current);
                                    ifCurrentNodeFull = btree2Functions3.ifCurrentNodeFull;
                                    if (ifCurrentNodeFull == true) {
                                        splitNode(nodeStack, current);
                                    }
                                    break loop3;
                                } else {
                                    break loop4;
                                }
                            }

                        } else {
                            //System.out.println("teen");
                            BTreeNode[] children = current.children;
                            current = children[i];
                            //nodeStack.push(current);
                            if (ifLeafNode(current)) {
                                //System.out.println("teen "+ ifLeafNode(current));
                                insertInNode(current, element, null, null, -1);
                                BTree btree2Functions3 = isNodeFull(current);
                                ifCurrentNodeFull = btree2Functions3.ifCurrentNodeFull;
                                if (ifCurrentNodeFull == true) {
                                    splitNode(nodeStack, current);
                                }
                                break loop3;
                            } else {
                                break loop4;
                            }
                        }
                    }
                }
            }
        }

    }

    public static ArrayList<Integer> asc(BTreeNode root,ArrayList<Integer> indexValues) {

//      Element[] rootElements1 = root.elements;
        //Visit the node by Printing the node data
//      for(int i=0; i<rootElements1.length; i++){
//          //System.out.print("hii "+rootElements1[i]+" ");
//      }
        BTreeNode[] children1 = root.children;

        if(root !=  null) {
            if(children1[0]==null){
                Element[] rootElements = root.elements;
                for(int i=0; i<rootElements.length; i++){
                    if(rootElements[i]!=null){
//                        System.out.println(data.get(rootElements[i].index));
                        indexValues.add(rootElements[i].index);
                    }
                    else{
                        break;
                    }
                }
            }
            else{
                int i;
                BTreeNode[] children = root.children;
                Element[] rootElements = root.elements;
                for(i=0; i< rootElements.length; i++){
                    if(rootElements[i]!=null){
                        if(children[i]!=null){
                            asc(children[i],indexValues);
//                            System.out.println(data.get(rootElements[i].index));
                            indexValues.add(rootElements[i].index);
                        }
                    }
                    else{
                        break;
                    }
                }
                if(children[i]!=null){
                    asc(children[i],indexValues);
                }
            }
        }
        return indexValues;
    }

    public static ArrayList<Integer> desc(BTreeNode root, ArrayList<Integer> indexValues) {

//      Element[] rootElements1 = root.elements;
        //Visit the node by Printing the node data
//      for(int i=0; i<rootElements1.length; i++){
//          //System.out.print("hii "+rootElements1[i]+" ");
//      }
        BTreeNode[] children1 = root.children;

        if(root !=  null) {
            if(children1[0]==null){
                Element[] rootElements = root.elements;
                for(int i=rootElements.length-1; i>=0; i--){
                    if(rootElements[i]!=null){
//                        System.out.println(data.get(rootElements[i].index));
                        indexValues.add(rootElements[i].index);
                    }
                }
            }
            else{
                int i;
                BTreeNode[] children = root.children;
                Element[] rootElements = root.elements;
                for(i=rootElements.length-1; i>=0; i--){
                    if(rootElements[i]!=null){
                        if(children[i+1]!=null){
                            desc(children[i+1],indexValues);
//                            System.out.println(data.get(rootElements[i].index));
                            indexValues.add(rootElements[i].index);
                        }
                    }
                }
                if(children[0]!=null){
                    desc(children[0],indexValues);
                }
            }
        }
        return indexValues;
    }

}