import { DataTypes, Model } from 'sequelize'

export default class BTCIndicator extends Model {
    static init(sequelize) {
        return super.init(
            {
                script:
                {
                    type: DataTypes.TEXT,
                    allowNull: false
                },
                timeframe:
                {
                    type: DataTypes.TEXT,
                    allowNull: false
                },
                open:
                {
                    type: DataTypes.FLOAT
                },
                high:
                {
                    type: DataTypes.FLOAT
                },
                low:
                {
                    type: DataTypes.FLOAT
                },
                close:
                {
                    type: DataTypes.FLOAT
                },
                timestamp:
                {
                    type: DataTypes.DATE
                },
                indicators:
                {
                    type: DataTypes.JSONB, allowNull: true
                },
            },
            {
                sequelize,
                underscored: true,
                paranoid: false,
                indexes: [
                    {
                        unique: true,
                        fields: ['script', 'timeframe', 'timestamp']
                    }
                ]
            }
        )
    }
}
