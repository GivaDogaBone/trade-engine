package com.george.quintas.tradeengine.controller;

import com.qsc.hazelcast.venuesservice.viridian.model.Venues;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

// https://www.javainuse.com/spring/boot_swagger3

@RestController
public class TradeEngineController {

	private List<Venues> venues = createList();

	@RequestMapping(value = "/venues", method = RequestMethod.GET, produces = "application/json")
	public List<Venues> firstPage() {
		return venues;
	}

	@DeleteMapping(path = { "/{id}" })
	public Venues delete(@PathVariable("id") int id) {
		Venues deletedVenue = null;
		for (Venues ven : venues) {
			if (ven.getEmpId().equals(id)) {
				venues.remove(ven);
				deletedVenue = ven;
				break;
			}
		}
		return deletedVenue;
	}

	@PostMapping
	public Venues create(@RequestBody Venues venue) {
		venues.add(venue);
		System.out.println(venues);
		return venue;
	}

	private static List<Venues> createList() {
		List<Venues> tempVenues = new ArrayList<>();
		Venues ven1 = new Venues();
		ven1.setName("ven1");
		ven1.setDesignation("manager");
		ven1.setEmpId("1");
		ven1.setSalary(3000);

		Venues ven2 = new Venues();
		ven2.setName("ven2");
		ven2.setDesignation("developer");
		ven2.setEmpId("2");
		ven2.setSalary(3000);
		tempVenues.add(ven1);
		tempVenues.add(ven2);
		return tempVenues;
	}
}