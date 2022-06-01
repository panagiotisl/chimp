package gr.aueb.delorean.chimp;

import java.util.List;

import gr.aueb.delorean.chimp.SwingFilter.SwingSegment;

public class DecompressorSwingFilter {

	private List<SwingSegment> swingSegments;
    private float storedVal = 0f;
    private boolean endOfStream = false;
    private int currentElement = 0;
    private int currentTimestampOffset = 0;

    public DecompressorSwingFilter(List<SwingSegment> swingSegments) {
    	this.swingSegments = swingSegments;
    }

    /**
     * Returns the next pair in the time series, if available.
     *
     * @return Pair if there's next value, null if series is done.
     */
    public Float readValue() {
        next();
        if(endOfStream) {
            return null;
        }
        return storedVal;
    }

    private void next() {
    	SwingSegment swingSegment = swingSegments.get(currentElement);
    	if (swingSegment.getFinalTimestamp() >= (swingSegment.getInitialTimestamp() + currentTimestampOffset)) {
    		storedVal = swingSegment.getLine().get(swingSegment.getInitialTimestamp() + currentTimestampOffset);
    		currentTimestampOffset++;
    	} else {
    		currentElement++;
    		if (currentElement < swingSegments.size()) {
    			swingSegment = swingSegments.get(currentElement);
    			storedVal = swingSegment.getLine().get(swingSegment.getInitialTimestamp());

    			currentTimestampOffset = 1;
    		} else {
    			endOfStream = true;
    		}
    	}
	}

}