import React from 'react';
import './MissingData.css';

const MissingData = ({ inline = false }) => {
    return (
        <span className={`missing-data ${inline ? 'inline' : ''}`}>
            NA
            <span className="missing-data-info">
                <span className="info-icon">i</span>
                <span className="missing-data-tooltip">
                    Data not updated in the database
                </span>
            </span>
        </span>
    );
};

export default MissingData;
