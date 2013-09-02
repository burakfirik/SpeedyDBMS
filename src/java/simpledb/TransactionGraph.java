package simpledb;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;


/***
 * Represents a graph. Each transaction Graph contains
 * a transaction object with links to the pages it uses.
 * Provides helper methods to detect conflicts etc
 * Currently linked to a HeapFile and HeapPageId
 * @author masonchang
 *
 */


class PageNode {
	PageNode(PageId pid) {
		assert (pid != null);
		assert (pid instanceof HeapPageId);


		HeapPageId heapPage = (HeapPageId) pid;
		this.pid = pid;
		this.pageNumber = heapPage.pageNumber();
		this.tableId = heapPage.getTableId();
		this.isWritable = false;
	}


	public PageId toPageId() {
		return new HeapPageId(this.tableId, this.pageNumber);
	}


	public void setWritable() {
		this.isWritable = true;
	}


	public boolean isWritable() {
		return this.isWritable;
	}


	public int getHash() {
		return this.pid.hashCode();
	}


	private PageId pid;
	private int pageNumber;
	private int tableId;
	private boolean isWritable;
}


class TransactionNode {
	TransactionId tid;
	TransactionNode(TransactionId tid) {
		this.tid = tid;
	}


	TransactionId toId() {
		return this.tid;
	}
}


/***
 * A Transaction graph is actually a bipartite graph.
 * One set is transactions
 * The other is the pages in use
 * If a page has a write lock, there should only be one edge between a 
 * transaction and a page. 
 * Read locks can have multiple transaction <--> page links
 * Each node has a read/write marker
 * @author masonchang
 *
 */
public class TransactionGraph {
	private HashMap<Long, TransactionNode> transactions;
	private HashMap<Integer, PageNode> pages;


	private HashMap<TransactionNode, HashSet<PageNode>> tidToPages;
	private HashMap<PageNode, HashSet<TransactionNode>> pageToTid;


	public TransactionGraph() {
		this.transactions = new HashMap<Long, TransactionNode>();
		this.pages = new HashMap<Integer, PageNode>();


		this.tidToPages = new HashMap<TransactionNode, HashSet<PageNode>>();
		this.pageToTid = new HashMap<PageNode, HashSet<TransactionNode>>();
	}


	/***
	 * Need the transaction / page wrappers because
	 * we can have multiple different VM objects that represent the same
	 * Transaction and page.
	 * @param tid
	 * @return
	 */
	private TransactionNode getTransactionNode(TransactionId tid) {
		long id = 0;
		if (tid != null) id = tid.getId();


		if (!this.transactions.containsKey(id)) {
			addTransactionNode(tid);
		}
		return this.transactions.get(id);
	}


	private PageNode getPageNode(PageId pid) {
		int id = pid.hashCode();
		if (!this.pages.containsKey(id)) {
			addPageNode(pid);
		}
		return this.pages.get(id);
	}


	private void addTransactionNode(TransactionId tid) {
		long id = 0;
		if (tid != null) id = tid.getId();


		
		this.transactions.put(id,new TransactionNode(tid));
	}


	private void addPageNode(PageId pid) {
		int hash = pid.hashCode();
		assert (!this.pages.containsKey(hash));
		this.pages.put(hash, new PageNode(pid));
	}


	private HashSet<PageNode> getTransactionToPageEdges(TransactionNode node) {
		if (!this.tidToPages.containsKey(node)) {
			this.tidToPages.put(node, new HashSet<PageNode>());
		}


		return this.tidToPages.get(node);
	}


	private HashSet<TransactionNode> getPageToTransactionEdges(PageNode node) {
		if (!this.pageToTid.containsKey(node)) {
			this.pageToTid.put(node, new HashSet<TransactionNode>());
		}


		return this.pageToTid.get(node);
	}


	private void clearTransactionNode(TransactionId tid) {
		long id = tid.getId();
		assert (this.transactions.containsKey(id));
		this.transactions.remove(id);
	}






	private void removeEdge(TransactionNode transactionNode, PageNode pageNode) {
		assert (getTransactionToPageEdges(transactionNode).contains(pageNode));


		getTransactionToPageEdges(transactionNode).remove(pageNode);
		getPageToTransactionEdges(pageNode).remove(transactionNode);
	}


	private void clearDeadPages() {
		for (PageNode node : this.pageToTid.keySet()) {
			if (getPageToTransactionEdges(node).isEmpty()) {
				this.pageToTid.remove(node);
			}
		}
	}


	public synchronized void transactionComplete(TransactionId tid) {
		TransactionNode transactionNode = getTransactionNode(tid);
		for (PageNode pageNode : getTransactionToPageEdges(transactionNode)) {
			removeEdge(transactionNode, pageNode);
		}


		clearTransactionNode(tid);
		clearDeadPages();
	}


	public synchronized HashSet<PageId> pagesInTransaction(TransactionId tid) {
		TransactionNode transactionNode = getTransactionNode(tid);
		HashSet<PageId> pages = new HashSet<PageId>();
		for (PageNode pageNode : getTransactionToPageEdges(transactionNode)) {
			pages.add(pageNode.toPageId());
		}


		return pages;
	}


	public synchronized HashSet<TransactionId> transactionsUsingPage(PageId pid) {
		PageNode node = getPageNode(pid);
		HashSet<TransactionId> transactions = new HashSet<TransactionId>();


		for (TransactionNode transactionNode : getPageToTransactionEdges(node)) {
			transactions.add(transactionNode.toId());
		}


		return transactions;
	}


	public synchronized  int numPages(TransactionId tid) {
		return pagesInTransaction(tid).size();
	}


	public synchronized int numTransactions(PageId pid) {
		return transactionsUsingPage(pid).size();
	}


	public synchronized boolean hasWriteLock(PageId pid) {
		return numTransactions(pid) == 1;
	}


	public synchronized void addEdge(TransactionId transaction, PageId pid) {
		TransactionNode transactionNode = getTransactionNode(transaction);
		PageNode page = getPageNode(pid);


		getTransactionToPageEdges(transactionNode).add(page);
		getPageToTransactionEdges(page).add(transactionNode);
	}


	public synchronized void removeEdge(TransactionId transaction, PageId pid) {
		TransactionNode transactionNode = getTransactionNode(transaction);
		PageNode pageNode = getPageNode(pid);
		removeEdge(transactionNode, pageNode);
	}


	public synchronized boolean readsPage(TransactionId tid, PageId pid) {
		TransactionNode node = getTransactionNode(tid);
		PageNode page = getPageNode(pid);
		return getTransactionToPageEdges(node).contains(page);
	}


	public synchronized boolean writesPage(TransactionId tid, PageId pid) {
		TransactionNode transactionNode = getTransactionNode(tid);
		PageNode pageNode = getPageNode(pid);


		HashSet<TransactionNode> nodes = getPageToTransactionEdges(pageNode);
		return (nodes.size() == 1) && (nodes.contains(transactionNode));


	}
}
