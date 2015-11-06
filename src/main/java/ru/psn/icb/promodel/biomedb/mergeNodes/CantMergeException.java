/**
 * 
 */
package ru.psn.icb.promodel.biomedb.mergeNodes;

/**
 * Exception to be thrown if two elements cannot be merged.
 * @author lptolik
 *
 */
public class CantMergeException extends Exception {

	/**
	 * 
	 */
	public CantMergeException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public CantMergeException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public CantMergeException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public CantMergeException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public CantMergeException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
