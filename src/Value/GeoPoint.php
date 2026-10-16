<?php

declare(strict_types=1);

namespace Ldkafka\GoogleFirestore\Value;

/**
 * A latitude and longitude in degrees (Firestore geoPointValue).
 */
final readonly class GeoPoint
{
    /**
     * @param float $latitude -90 to 90.
     * @param float $longitude -180 to 180.
     */
    public function __construct(public float $latitude, public float $longitude)
    {
    }
}
